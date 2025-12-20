# Company Similarity Bug Analysis

## Issue

After running `compute_company_similarity.py`, expected relationships are missing:
- KO (Coca-Cola) and PEP (Pepsi) should have `SIMILAR_SIZE` relationship (both in >$10B revenue bucket with 737 companies)
- KO should have ~736 `SIMILAR_SIZE` relationships from the revenue bucket alone
- KO only has 3 `SIMILAR_SIZE` relationships in the database
- PEP is ranked #8 for KO similarity when it should be #1 or #2

## Root Cause

The `compute_size_similarity` function appears to work correctly with small datasets (tested with 2-12 companies), but fails to generate all expected pairs when processing all 7,209 companies.

**Evidence:**
- KO and PEP are both in the same buckets for all metrics:
  - Revenue >$10B: 737 companies (both present)
  - Market Cap >$10B: 900 companies (both present)
  - Employees >10000: 986 companies (both present)
- Manual pair generation from the >$10B revenue bucket correctly finds KO-PEP pair
- But when `compute_size_similarity` runs on all companies, KO-PEP pair is NOT generated
- KO only appears in 5 pairs total (should be 736+ from revenue bucket alone)

## Current Status

- ✅ `SIMILAR_INDUSTRY` relationships work correctly (KO-PEP have this)
- ✅ Composite similarity query works (shows PEP at #8)
- ❌ `SIMILAR_SIZE` relationships are incomplete (missing KO-PEP and many others)

## Workaround

Use the composite similarity query to find similar companies. Even without `SIMILAR_SIZE`, PEP still appears in top 10 for KO:

```cypher
MATCH (c1:Company {ticker: 'KO'})-[r]-(c2:Company)
WHERE type(r) IN ['SIMILAR_INDUSTRY', 'SIMILAR_SIZE', 'SIMILAR_DESCRIPTION',
                  'SIMILAR_TECHNOLOGY']
WITH c2,
     count(r) as edge_count,
     sum(CASE type(r)
         WHEN 'SIMILAR_INDUSTRY' THEN 1.0
         WHEN 'SIMILAR_SIZE' THEN 0.8
         WHEN 'SIMILAR_DESCRIPTION' THEN 0.9
         WHEN 'SIMILAR_TECHNOLOGY' THEN 0.7
         ELSE 0.0
     END) as weighted_score
RETURN c2.ticker, c2.name, edge_count, weighted_score
ORDER BY weighted_score DESC, edge_count DESC
LIMIT 20
```

## Next Steps to Fix

1. **Debug pair generation**: Add logging to `compute_size_similarity` to see why pairs aren't being generated for large buckets
2. **Check for limits**: Verify there are no implicit limits on pair generation
3. **Verify CIK matching**: Ensure CIKs are consistently formatted (strings) throughout
4. **Test with subset**: Run computation on just the >$10B revenue bucket to isolate the issue
5. **Consider batching**: If memory is an issue, process buckets in batches

## Expected Behavior

Once fixed, KO should have:
- ~736 `SIMILAR_SIZE` relationships from revenue bucket
- ~899 `SIMILAR_SIZE` relationships from market_cap bucket
- ~985 `SIMILAR_SIZE` relationships from employees bucket
- (Deduplicated to ~1000-1500 unique relationships)

PEP should rank #1 or #2 for KO similarity with a composite score of ~1.8 (SIMILAR_INDUSTRY: 1.0 + SIMILAR_SIZE: 0.8).
