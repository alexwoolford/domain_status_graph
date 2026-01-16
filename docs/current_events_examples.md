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

## Example 1: Red Sea Shipping Disruptions → E-commerce Companies

**Event**: Houthi attacks on Red Sea shipping routes (2024)

**Surprising Connection**: The graph identifies e-commerce companies through their technology stack (Shopify), revealing which specific companies have global supply chain dependencies that aren't obvious from company descriptions alone.

**Graph Query**:
```cypher
// Find companies using Shopify (e-commerce platform)
MATCH (c:Company)-[:HAS_DOMAIN]->(d:Domain)-[:USES]->(t:Technology {name: 'Shopify'})
RETURN c.ticker, c.name, c.sector
LIMIT 20
```

**Impact Chain**:
1. **Direct Impact**: Shipping disruptions affect global logistics
2. **First-Order Impact**: E-commerce companies using Shopify:
   - **Allbirds (BIRD)** - Direct-to-consumer footwear
   - **Arhaus (ARHS)** - Furniture retailer
   - **Constellation Brands (STZ)** - Consumer goods
   - **Edgewell Personal Care (EPC)** - Consumer products
3. **Second-Order Impact**: Companies with similar business models (via `SIMILAR_DESCRIPTION`):
   - Other D2C brands
   - Companies with similar supply chain structures
   - Retailers dependent on global shipping

**Why It's Surprising**: The graph's value is in **identifying which specific e-commerce companies are exposed** through their technology stack, even when their global supply chain dependencies aren't explicitly mentioned in company descriptions. Technology choices (Shopify) serve as a signal for companies with global shipping needs.

---

## Example 2: Boeing Production Delays → Airlines & Defense Contractors

**Event**: Boeing 737 MAX production issues and quality control problems (2024)

**Surprising Connection**: Defense contractors and aerospace suppliers are impacted through shared supply chains.

**Graph Query**:
```cypher
// Find companies that depend on Boeing as a supplier
MATCH (c:Company)-[:HAS_SUPPLIER]->(ba:Company {ticker: 'BA'})
RETURN c.ticker, c.name, c.sector
```

**Impact Chain**:
1. **Direct Impact**: Boeing production delays
2. **First-Order Impact**: Airlines directly dependent on Boeing:
   - **United Airlines (UAL)** - "sources majority of aircraft from Boeing"
   - **Southwest Airlines (LUV)** - Boeing 737 fleet
   - **Air Lease Corp (AL)** - Aircraft leasing
3. **Second-Order Impact**: Aerospace suppliers and defense contractors:
   - **General Electric (GE)** - Aircraft engines
   - **Moog Inc. (MOG-A)** - Aerospace components
   - **Textron (TXT)** - Aerospace systems
   - **SIFCO Industries (SIF)** - Aerospace manufacturing

**Why It's Surprising**: Defense contractors (GE, Moog) share the same supplier (Boeing) as commercial airlines, creating unexpected exposure to commercial aviation disruptions.

---

## Example 3: China Rare Earth Export Controls → EV Manufacturers

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

**Why It's Surprising**: The graph reveals that solar companies (FTC Solar) share similar risk profiles to EV companies, both dependent on rare earth magnets for power electronics.

---

## Example 4: Oracle Cloud Outage → Government Contractors

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

**Why It's Surprising**: E-commerce companies (Brilliant Earth) and government contractors (Telos) share the same critical infrastructure dependency (Oracle), creating unexpected systemic risk.

---

## Example 5: Helium Shortage → Medical Device Companies

**Event**: Global helium supply shortages (2023-2024)

**Surprising Connection**: Medical device companies are impacted through industrial gas supply chains, not just MRI manufacturers.

**Graph Query**:
```cypher
// Find companies similar to GE HealthCare (medical imaging)
MATCH (gehc:Company {ticker: 'GEHC'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.70
RETURN similar.ticker, similar.name, r.score
ORDER BY r.score DESC
LIMIT 10
```

**Impact Chain**:
1. **Direct Impact**: Helium supply shortages (Russia, Qatar disruptions)
2. **First-Order Impact**: Industrial gas companies:
   - **Air Products (APD)** - Helium liquefaction
   - **Linde (LIN)** - Helium distribution
3. **Second-Order Impact**: Medical device companies:
   - **GE HealthCare (GEHC)** - MRI systems (liquid helium)
   - Companies similar to GEHC (via `SIMILAR_DESCRIPTION`):
     - **Teleflex (TFX)** - Medical technology
     - Other medical device manufacturers
4. **Third-Order Impact**: Healthcare providers and hospitals (via customer relationships)

**Why It's Surprising**: The graph reveals that medical device companies beyond just MRI manufacturers are exposed, through similarity relationships that indicate shared technology dependencies.

---

## Example 6: Semiconductor Export Controls → Cross-Industry Exposure

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

## Example 7: Middle East Conflict → Shipping & Logistics Companies

**Event**: Houthi attacks on Red Sea shipping, Panama Canal drought (2024)

**Surprising Connection**: The graph identifies e-commerce and consumer goods companies through their technology stack, revealing which specific companies have global shipping dependencies that may not be obvious from company descriptions.

**Graph Query**:
```cypher
// Find companies using Shopify (indicating e-commerce, global shipping needs)
MATCH (c:Company)-[:HAS_DOMAIN]->(d:Domain)-[:USES]->(t:Technology {name: 'Shopify'})
WITH c
MATCH (c)-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.70
RETURN DISTINCT similar.ticker, similar.name, similar.sector
LIMIT 15
```

**Impact Chain**:
1. **Direct Impact**: Red Sea shipping disruptions, Panama Canal restrictions
2. **First-Order Impact**: E-commerce companies using Shopify:
   - **Allbirds (BIRD)** - Global footwear supply chain
   - **Arhaus (ARHS)** - Furniture (heavy shipping)
   - **Constellation Brands (STZ)** - Consumer goods
3. **Second-Order Impact**: Companies with similar business models:
   - Via `SIMILAR_DESCRIPTION` relationships
   - D2C brands with similar supply chain structures
   - Consumer goods companies with global manufacturing

**Why It's Surprising**: The graph's value is in **identifying which specific companies are exposed** through technology stack analysis, even when global shipping dependencies aren't explicitly mentioned. Technology choices (Shopify) serve as a signal for companies with global supply chain needs.

---

## Example 9: Defense Budget Changes → Commercial Aerospace

**Event**: U.S. defense budget shifts or procurement changes

**Surprising Connection**: Commercial airlines and defense contractors share suppliers (Boeing, Lockheed Martin).

**Graph Query**:
```cypher
// Find companies that depend on defense contractors as suppliers
MATCH (c:Company)-[:HAS_SUPPLIER]->(s:Company)
WHERE s.ticker IN ['BA', 'LMT', 'RTX', 'NOC']
RETURN c.ticker, c.name, c.sector, s.ticker, s.name
```

**Impact Chain**:
1. **Direct Impact**: Defense budget changes affect Lockheed Martin, Raytheon, Northrop Grumman
2. **First-Order Impact**: Defense contractors:
   - **Lockheed Martin (LMT)** - Defense systems
   - **Boeing (BA)** - Both commercial and defense
3. **Second-Order Impact**: Companies dependent on these suppliers:
   - **United Airlines (UAL)** - Boeing commercial aircraft
   - **Southwest Airlines (LUV)** - Boeing 737 fleet
   - **Sidus Space (SIDU)** - Space systems (Lockheed supplier)
   - **Griffon Corp (GFF)** - Aerospace components
   - **Bruker Corp (BRKR)** - Scientific instruments (defense applications)

**Why It's Surprising**: Commercial airlines (United, Southwest) share the same supplier (Boeing) as defense contractors, creating unexpected exposure to defense budget changes through production capacity constraints.

---

## Example 9: Tariffs on Chinese Imports → Technology Companies

**Event**: Increased tariffs on Chinese imports (2024-2025)

**Surprising Connection**: Technology companies with similar technology stacks may share similar supply chain structures, revealing indirect exposure to Chinese import tariffs even when direct supplier relationships aren't disclosed.

**Graph Query**:
```cypher
// Find companies with similar technology stacks (may indicate similar supply chains)
MATCH (c1:Company)-[:HAS_DOMAIN]->(d1:Domain)-[:USES]->(t:Technology)
WITH c1, collect(DISTINCT t.name) as tech_stack
MATCH (c2:Company)-[:HAS_DOMAIN]->(d2:Domain)-[:USES]->(t)
WITH c1, c2, tech_stack, collect(DISTINCT t.name) as tech_stack2
WHERE c1 <> c2 AND size(apoc.coll.intersection(tech_stack, tech_stack2)) > 5
RETURN c1.ticker, c1.name, c2.ticker, c2.name,
       size(apoc.coll.intersection(tech_stack, tech_stack2)) as shared_tech
LIMIT 10
```

**Alternative Approach** (using similarity relationships):
```cypher
// Find companies with similar technology stacks
MATCH (c1:Company)-[r:SIMILAR_TECHNOLOGY]->(c2:Company)
WHERE r.score > 0.60
RETURN c1.ticker, c1.name, c2.ticker, c2.name, r.score
ORDER BY r.score DESC
LIMIT 10
```

**Impact Chain**:
1. **Direct Impact**: Tariffs on Chinese imports
2. **First-Order Impact**: Companies directly sourcing from China (if disclosed in 10-Ks)
3. **Second-Order Impact**: Companies with similar technology stacks:
   - Via `SIMILAR_TECHNOLOGY` relationships
   - Companies using similar components may have similar supply chains
4. **Third-Order Impact**: Companies competing with tariff-exposed firms:
   - Via `HAS_COMPETITOR` relationships
   - Competitive advantage shifts

**Why It's Surprising**: The graph can reveal indirect exposure through technology stack similarities, even when direct supplier relationships aren't disclosed. Companies with similar technology stacks often share similar supply chain structures, creating exposure clusters that aren't obvious from individual company disclosures.

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

2. **Technology Stack Clustering**: Companies using the same technologies (Shopify, NVIDIA GPUs) often have similar supply chain risks

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
