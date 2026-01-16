# Example Quality Review

## Assessment Criteria

For an example to be "good," it should:
1. **Reveal non-obvious connections** - Not something you'd immediately guess
2. **Demonstrate graph value** - Shows what the graph can do that traditional databases can't
3. **Be reproducible** - All connections exist in the graph
4. **Have clear impact chain** - Logical progression from event to companies

---

## Example-by-Example Review

### ✅ **Example 3: Boeing → Airlines & Defense Contractors**
**Quality: Excellent**

**Why it's good:**
- **Genuinely surprising**: Defense contractors (GE, Moog) sharing Boeing as a supplier with commercial airlines is non-obvious
- **Cross-industry exposure**: Shows how defense budget changes can impact commercial aviation
- **Clear impact chain**: Boeing delays → Airlines (obvious) → Defense contractors (surprising)
- **Reproducible**: Direct `HAS_SUPPLIER` relationships verified

**Verdict: Keep as-is**

---

### ✅ **Example 5: Oracle → Government Contractors**
**Quality: Good**

**Why it's good:**
- **Surprising pairing**: Brilliant Earth (e-commerce) + Telos (government contractor) both depend on Oracle
- **Systemic risk**: Shows how infrastructure dependencies create unexpected clusters
- **Reproducible**: Direct relationships verified

**Minor issue**: The government contractor/Oracle connection itself is somewhat expected, but the e-commerce + government contractor pairing is surprising.

**Verdict: Keep, but could emphasize the surprising pairing more**

---

### ⚠️ **Example 1: AI Chip Restrictions → Cryptocurrency Miners**
**Quality: Weak**

**Issues:**
- **Not surprising**: Crypto miners using NVIDIA GPUs is well-known
- **Obvious connection**: Anyone familiar with crypto mining knows they use GPUs

**What's actually valuable:**
- The graph reveals **specific companies** (Bit Digital, IREN) and their relationships
- Shows **multi-hop impacts** (similar companies via SIMILAR_DESCRIPTION)
- Reveals **unexpected pairings** (data centers + crypto miners share supplier)

**Recommendation**:
- **Reframe** to emphasize: "The graph reveals which specific crypto mining companies are exposed, and shows they share suppliers with data center operators (unexpected pairing)"
- Or **replace** with a more surprising example

**Verdict: Needs reframing or replacement**

---

### ❌ **Example 2: Red Sea Shipping → Shopify**
**Quality: Weak**

**Issues:**
- **Tenuous connection**: Shopify is just a software platform, not a shipping company
- **Circular logic**: E-commerce companies need shipping → Shopify users are e-commerce → Therefore Shopify users are exposed
- **Not graph-specific**: You could make the same argument for any e-commerce company, Shopify or not
- **Weak causality**: Using Shopify doesn't cause shipping exposure; being an e-commerce company does

**What's actually happening:**
- The graph is using Shopify as a **proxy** for "e-commerce company"
- But this is a weak proxy - not all e-commerce uses Shopify, and not all Shopify users have global shipping needs

**Recommendation**:
- **Remove** this example
- Or **reframe** to: "E-commerce companies (identified via technology stack) are exposed to shipping disruptions" - but this is still obvious

**Verdict: Remove or significantly reframe**

---

### ⚠️ **Example 4: Rare Earth → EV Manufacturers**
**Quality: Mixed**

**What's good:**
- **Solar + EV connection**: FTC Solar sharing risk profile with Tesla is genuinely interesting
- **Multi-hop supply chain**: Shows how events cascade through supply chains

**What's obvious:**
- EV manufacturers using rare earth magnets is well-known
- The basic connection is expected

**Recommendation**:
- **Emphasize the surprising part**: Solar companies (FTC Solar) sharing risk with EV companies
- **De-emphasize** the obvious EV/rare earth connection

**Verdict: Keep but reframe to emphasize solar/EV connection**

---

### ⚠️ **Example 6: Helium → Medical Devices**
**Quality: Mixed**

**What's good:**
- **Beyond MRI**: Shows medical device companies beyond just MRI manufacturers are exposed
- **Similarity relationships**: Demonstrates how SIMILAR_DESCRIPTION reveals shared dependencies

**What's obvious:**
- MRI machines needing helium is well-known

**Recommendation**:
- **Emphasize**: "Medical device companies beyond just MRI manufacturers are exposed"
- Make it clear the surprise is the **breadth** of exposure, not the basic connection

**Verdict: Keep but reframe to emphasize breadth of exposure**

---

### ⚠️ **Example 7: Semiconductor Controls → Data Centers**
**Quality: Mixed**

**What's good:**
- **Unexpected pairing**: Enterprise network equipment (Arista, Fortinet) sharing suppliers with crypto miners is interesting
- **Cross-industry exposure**: Shows how semiconductor restrictions impact diverse industries

**What's obvious:**
- Crypto miners using NVIDIA is obvious (same issue as Example 1)

**Recommendation**:
- **Emphasize**: Enterprise network equipment manufacturers (Arista, Fortinet) sharing suppliers with crypto miners
- **De-emphasize** the crypto/NVIDIA connection

**Verdict: Keep but reframe to emphasize enterprise/crypto pairing**

---

### ❌ **Example 8: Middle East Conflict → Shopify**
**Quality: Weak**

**Same issues as Example 2** - weak connection, circular logic, not graph-specific.

**Verdict: Remove (duplicate of Example 2)**

---

### ✅ **Example 9: Defense Budget → Commercial Aerospace**
**Quality: Good**

**Why it's good:**
- **Genuinely surprising**: Commercial airlines exposed to defense budget changes through shared suppliers
- **Cross-industry exposure**: Defense policy impacts commercial aviation
- **Clear impact chain**: Defense budget → Defense contractors → Shared suppliers → Airlines

**Verdict: Keep as-is**

---

### ⚠️ **Example 10: Tariffs → Technology Companies**
**Quality: Weak**

**Issues:**
- **Hypothetical query**: The query won't work (Chinese companies not in graph)
- **Vague connection**: "Technology companies using similar components" is too generic
- **Not reproducible**: Can't actually run the query as written

**Recommendation**:
- **Remove** or **significantly reframe** with a working query
- Focus on what CAN be queried (similar technology stacks, not Chinese suppliers)

**Verdict: Remove or significantly reframe**

---

## Summary

### Keep (Strong Examples):
- ✅ **Example 3**: Boeing → Airlines & Defense Contractors
- ✅ **Example 5**: Oracle → Government Contractors (emphasize surprising pairing)
- ✅ **Example 9**: Defense Budget → Commercial Aerospace

### Keep but Reframe (Mixed Quality):
- ⚠️ **Example 1**: AI Chip → Crypto (emphasize specific companies + unexpected pairings)
- ⚠️ **Example 4**: Rare Earth → EV (emphasize solar/EV connection)
- ⚠️ **Example 6**: Helium → Medical Devices (emphasize breadth beyond MRI)
- ⚠️ **Example 7**: Semiconductor → Data Centers (emphasize enterprise/crypto pairing)

### Remove (Weak Examples):
- ❌ **Example 2**: Red Sea Shipping → Shopify
- ❌ **Example 8**: Middle East Conflict → Shopify (duplicate)
- ❌ **Example 10**: Tariffs → Technology Companies (hypothetical, not reproducible)

---

## Recommendations

1. **Remove weak examples** (2, 8, 10) - they undermine the quality of the document
2. **Reframe obvious connections** (1, 4, 6, 7) to emphasize what's actually surprising
3. **Add new examples** that are genuinely surprising:
   - Focus on cross-industry exposure
   - Emphasize unexpected pairings
   - Show multi-hop impacts that aren't obvious

4. **Quality over quantity**: Better to have 5-6 strong examples than 10 mixed-quality ones
