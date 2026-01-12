# Extraction Bug Fixes - 2026-01-12

## Summary

**Two critical bugs fixed** that prevented business relationship extraction from working:

1. **Bug #1**: TieredDecisionSystem blocking valid companies (generic word blocklist)
2. **Bug #2**: Wrong relationship type format passed to confidence tier function

**Result**: Extraction now works correctly! ✅

---

## Bug #1: Generic Word Blocklist

### Problem
`TieredDecisionSystem` Tier 1 was blocking valid company names ("Microsoft", "Apple", "Amazon", "Google") because they were in the `generic_words` blocklist, even though they had successfully resolved to companies.

### Root Cause
The blocklist check didn't account for successful entity resolution. If a candidate resolved to a company, it should skip the blocklist.

### Fix
**File**: `public_company_graph/entity_resolution/tiered_decision.py`

Modified `_tier1_decide()` to accept `company_name` parameter. If `company_name` is provided (indicating successful entity resolution), skip the generic word blocklist.

**Before**:
```python
def _tier1_decide(self, candidate, context):
    if mention in generic_words:
        if self._is_in_company_list(context, mention):
            return None
        else:
            return TieredDecision(decision=Decision.REJECT, ...)  # BLOCKS VALID COMPANIES
```

**After**:
```python
def _tier1_decide(self, candidate, context, company_name=None):
    if mention in generic_words:
        if company_name:  # Entity resolution succeeded - it's a real company
            return None  # Skip blocklist
        if self._is_in_company_list(context, mention):
            return None
        else:
            return TieredDecision(decision=Decision.REJECT, ...)
```

---

## Bug #2: Wrong Relationship Type Format

### Problem
`extract_and_resolve_relationships` was passing `relationship_type.value` (e.g., "competitor") to `TieredDecisionSystem.decide()`, which then passed it to `get_confidence_tier()`. However, `get_confidence_tier()` expects the Neo4j format (e.g., "HAS_COMPETITOR").

### Root Cause
- `RelationshipType.COMPETITOR.value` = `"competitor"`
- `get_confidence_tier("competitor", 0.4)` returns `ConfidenceTier.LOW` (not found in config)
- `get_confidence_tier("HAS_COMPETITOR", 0.4)` returns `ConfidenceTier.HIGH` (correct)

This caused Tier 3 to always return `REJECT` instead of `ACCEPT` for valid relationships.

### Fix
**File**: `public_company_graph/parsing/business_relationship_extraction.py`

Convert relationship type to Neo4j format before passing to decision system:

**Before**:
```python
tiered_decision = decision_system.decide(
    candidate=candidate_obj,
    context=sentence[:500],
    relationship_type=relationship_type.value,  # "competitor" - WRONG!
    company_name=resolved["name"],
    embedding_similarity=embedding_similarity,
    llm_verifier=llm_verifier,
)
```

**After**:
```python
# Convert relationship_type to Neo4j format (e.g., "competitor" -> "HAS_COMPETITOR")
neo4j_relationship_type = RELATIONSHIP_TYPE_TO_NEO4J.get(relationship_type, f"HAS_{relationship_type.name}")
tiered_decision = decision_system.decide(
    candidate=candidate_obj,
    context=sentence[:500],
    relationship_type=neo4j_relationship_type,  # "HAS_COMPETITOR" - CORRECT!
    company_name=resolved["name"],
    embedding_similarity=embedding_similarity,
    llm_verifier=llm_verifier,
)
```

---

## Verification

### Before Fixes
- **Microsoft**: 0 competitors extracted
- **Google**: 0 competitors extracted
- **Apple**: 0 competitors extracted

### After Fixes
- **Microsoft**: 5 competitors extracted ✅
  - Apple Inc. (sim=0.453)
  - CISCO SYSTEMS, INC. (sim=0.463)
  - ORACLE CORP (sim=0.424)
  - ADOBE INC. (sim=0.392)
  - (1 more)
- **Google**: 2 competitors extracted ✅
  - MICROSOFT CORP (sim=0.415)
  - Apple Inc. (sim=0.447)
- **Apple**: 0 competitors (no mentions in business description)

### Test Results
- ✅ All unit tests pass
- ✅ No linter errors
- ✅ Extraction works correctly

---

## Impact

**Before**: 0 business relationships created (entire extraction pipeline broken)
**After**: Relationships are now being extracted correctly

**Expected Results** (when running full extraction):
- ~3,000+ HAS_COMPETITOR relationships
- ~500+ HAS_PARTNER relationships
- ~100+ HAS_CUSTOMER relationships
- ~30+ HAS_SUPPLIER relationships

---

## Next Steps

1. **Re-run full extraction**:
   ```bash
   python scripts/extract_with_llm_verification.py --clean --execute
   ```

2. **Verify relationships created**:
   ```bash
   python sanity_check_graph.py
   ```

3. **Expected**: Thousands of business relationships should now be created successfully.
