#!/usr/bin/env python3
"""
Verify parsed 10-K data and embeddings quality.

Pulls sample records to confirm:
- Business descriptions were parsed
- Risk factors were parsed
- Websites were extracted
- Embeddings are valid and high quality
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import numpy as np

from public_company_graph.cache import get_cache
from public_company_graph.cli import get_driver_and_database, setup_logging

logger = setup_logging("verify_parsed_data")
driver, database = get_driver_and_database(logger)
cache = get_cache()

print("=" * 80)
print("SAMPLE RECORDS: 10-K PARSED DATA (FROM CACHE)")
print("=" * 80)
print()

# Get sample CIKs from cache
total_cached = cache.count("10k_extracted")
all_ciks = cache.keys("10k_extracted", limit=total_cached)
sample_ciks = all_ciks[:5]  # First 5

for i, cik in enumerate(sample_ciks, 1):
    data = cache.get("10k_extracted", cik)
    if not data:
        continue

    print(f"Sample {i}: CIK {cik}")
    print("-" * 80)

    # Website
    website = data.get("website", "N/A")
    print(f"Website: {website}")

    # Business Description
    desc = data.get("business_description", "")
    if desc:
        desc_preview = desc[:500] + "..." if len(desc) > 500 else desc
        print(f"Business Description: {len(desc):,} chars")
        print(f"  Preview: {desc_preview}")
    else:
        print("Business Description: Not found")

    # Risk Factors
    risks = data.get("risk_factors", "")
    if risks:
        risks_preview = risks[:500] + "..." if len(risks) > 500 else risks
        print(f"Risk Factors: {len(risks):,} chars")
        print(f"  Preview: {risks_preview}")
    else:
        print("Risk Factors: Not found")

    print()

# Statistics
print("=" * 80)
print("PARSING STATISTICS (FROM CACHE)")
print("=" * 80)
print()
with_desc = 0
with_risks = 0
with_website = 0
with_all = 0

for cik in all_ciks:
    data = cache.get("10k_extracted", cik)
    if not data:
        continue

    has_desc = bool(data.get("business_description"))
    has_risks = bool(data.get("risk_factors"))
    has_web = bool(data.get("website"))

    if has_desc:
        with_desc += 1
    if has_risks:
        with_risks += 1
    if has_web:
        with_website += 1
    if has_desc and has_risks and has_web:
        with_all += 1

print(f"Total companies in 10-K cache: {total_cached}")
print(f"Companies with business description: {with_desc} ({with_desc / total_cached * 100:.1f}%)")
print(f"Companies with risk factors: {with_risks} ({with_risks / total_cached * 100:.1f}%)")
print(f"Companies with website: {with_website} ({with_website / total_cached * 100:.1f}%)")
print(f"Companies with all three: {with_all} ({with_all / total_cached * 100:.1f}%)")
print()

# Neo4j statistics
print("=" * 80)
print("NEO4J STATISTICS")
print("=" * 80)
print()

with driver.session(database=database) as session:
    result = session.run(
        """
        MATCH (c:Company)
        RETURN
            count(c) AS total_companies,
            count(c.business_description_10k) AS with_10k_desc,
            count(c.description_embedding) AS with_embeddings,
            avg(size(c.business_description_10k)) AS avg_desc_length,
            min(size(c.business_description_10k)) AS min_desc_length,
            max(size(c.business_description_10k)) AS max_desc_length
        """
    )
    stats = result.single()

    print(f"Total companies: {stats['total_companies']}")
    print(f"Companies with 10-K descriptions: {stats['with_10k_desc']}")
    print(f"Companies with embeddings: {stats['with_embeddings']}")
    print()
    print("Description length statistics:")
    if stats["avg_desc_length"]:
        print(f"  Average: {stats['avg_desc_length']:,.0f} chars")
        print(f"  Min: {stats['min_desc_length']:,} chars")
        print(f"  Max: {stats['max_desc_length']:,} chars")
    print()

# Sample companies with embeddings
print("=" * 80)
print("SAMPLE RECORDS: COMPANY NODES WITH EMBEDDINGS")
print("=" * 80)
print()

with driver.session(database=database) as session:
    result = session.run(
        """
        MATCH (c:Company)
        WHERE c.description_embedding IS NOT NULL
          AND c.business_description_10k IS NOT NULL
        RETURN c.cik AS cik,
               c.ticker AS ticker,
               c.name AS name,
               size(c.business_description_10k) AS desc_length,
               size(c.description_embedding) AS embedding_dim,
               c.embedding_model AS model
        LIMIT 5
    """
    )

    companies = []
    for record in result:
        companies.append(record)
        print(f"Sample: {record['name']} ({record['ticker']})")
        print("-" * 80)
        print(f"CIK: {record['cik']}")
        print(f"Description length: {record['desc_length']:,} chars")
        print(f"Embedding dimensions: {record['embedding_dim']}")
        print(f"Embedding model: {record['model'] or 'N/A'}")
        print()

# Embedding quality check
print("=" * 80)
print("EMBEDDING QUALITY CHECK")
print("=" * 80)
print()

with driver.session(database=database) as session:
    result = session.run(
        """
        MATCH (c:Company)
        WHERE c.description_embedding IS NOT NULL
          AND c.business_description_10k IS NOT NULL
        RETURN c.cik AS cik,
               c.name AS name,
               c.ticker AS ticker,
               c.description_embedding AS embedding,
               size(c.business_description_10k) AS desc_length
        LIMIT 5
    """
    )

    company_embeddings = []
    for record in result:
        embedding = record["embedding"]
        if embedding and isinstance(embedding, list):
            company_embeddings.append(
                {
                    "cik": record["cik"],
                    "name": record["name"],
                    "ticker": record["ticker"],
                    "embedding": np.array(embedding, dtype=np.float32),
                    "desc_length": record["desc_length"],
                }
            )

    print(f"Checking {len(company_embeddings)} companies with embeddings:")
    print()

    for i, company in enumerate(company_embeddings, 1):
        emb = company["embedding"]

        print(f"{i}. {company['name']} ({company['ticker']})")
        print(f"   Description: {company['desc_length']:,} chars")
        print(f"   Embedding dimensions: {len(emb)}")
        print(f"   Embedding norm: {np.linalg.norm(emb):.4f}")
        print(f"   Embedding min: {emb.min():.4f}, max: {emb.max():.4f}")
        print(f"   Embedding mean: {emb.mean():.4f}, std: {emb.std():.4f}")

        # Check for NaN or Inf
        has_nan = np.isnan(emb).any()
        has_inf = np.isinf(emb).any()
        print(f"   Has NaN: {has_nan}, Has Inf: {has_inf}")
        print()

    # Check similarity between embeddings
    if len(company_embeddings) >= 2:
        print("Similarity check (cosine similarity):")
        for i in range(min(3, len(company_embeddings) - 1)):
            emb1 = company_embeddings[i]["embedding"]
            emb2 = company_embeddings[i + 1]["embedding"]

            # Normalize
            norm1 = np.linalg.norm(emb1)
            norm2 = np.linalg.norm(emb2)
            if norm1 > 0 and norm2 > 0:
                similarity = np.dot(emb1, emb2) / (norm1 * norm2)
                print(
                    f"  {company_embeddings[i]['name']} vs {company_embeddings[i + 1]['name']}: {similarity:.4f}"
                )
        print()
        print("(Similarity should be between -1 and 1, typically 0.3-0.9 for similar companies)")

driver.close()
