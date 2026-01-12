// Edge Quality Audit - Run this directly in Neo4j Browser or Cypher Shell
// This provides the same information as the Python script

// First, let's see overall edge counts by type
MATCH ()-[r]->()
WHERE type(r) IN ['HAS_COMPETITOR', 'HAS_PARTNER', 'HAS_SUPPLIER', 'HAS_CUSTOMER']
RETURN type(r) AS relationship_type, count(*) AS total_edges
ORDER BY total_edges DESC;

// Now let's audit HAS_COMPETITOR edges
// High threshold: 0.35, Medium threshold: 0.25
MATCH (source:Company)-[r:HAS_COMPETITOR]->(target:Company)
WITH
    count(*) AS total,
    sum(CASE WHEN r.embedding_similarity IS NULL THEN 1 ELSE 0 END) AS no_embedding,
    sum(CASE WHEN r.embedding_similarity >= 0.35 THEN 1 ELSE 0 END) AS high_confidence,
    sum(CASE WHEN r.embedding_similarity >= 0.25 AND r.embedding_similarity < 0.35 THEN 1 ELSE 0 END) AS medium_confidence,
    sum(CASE WHEN r.embedding_similarity < 0.25 OR r.embedding_similarity IS NULL THEN 1 ELSE 0 END) AS low_confidence,
    sum(CASE WHEN r.embedding_similarity < 0.35 THEN 1 ELSE 0 END) AS below_high_threshold
RETURN
    'HAS_COMPETITOR' AS relationship_type,
    total,
    high_confidence,
    round(100.0 * high_confidence / total, 1) AS high_pct,
    medium_confidence,
    round(100.0 * medium_confidence / total, 1) AS medium_pct,
    low_confidence,
    round(100.0 * low_confidence / total, 1) AS low_pct,
    no_embedding,
    round(100.0 * no_embedding / total, 1) AS no_embedding_pct,
    below_high_threshold,
    round(100.0 * below_high_threshold / total, 1) AS below_threshold_pct;

// Audit HAS_PARTNER edges
// High threshold: 0.50, Medium threshold: 0.30
MATCH (source:Company)-[r:HAS_PARTNER]->(target:Company)
WITH
    count(*) AS total,
    sum(CASE WHEN r.embedding_similarity IS NULL THEN 1 ELSE 0 END) AS no_embedding,
    sum(CASE WHEN r.embedding_similarity >= 0.50 THEN 1 ELSE 0 END) AS high_confidence,
    sum(CASE WHEN r.embedding_similarity >= 0.30 AND r.embedding_similarity < 0.50 THEN 1 ELSE 0 END) AS medium_confidence,
    sum(CASE WHEN r.embedding_similarity < 0.30 OR r.embedding_similarity IS NULL THEN 1 ELSE 0 END) AS low_confidence,
    sum(CASE WHEN r.embedding_similarity < 0.50 THEN 1 ELSE 0 END) AS below_high_threshold
RETURN
    'HAS_PARTNER' AS relationship_type,
    total,
    high_confidence,
    round(100.0 * high_confidence / total, 1) AS high_pct,
    medium_confidence,
    round(100.0 * medium_confidence / total, 1) AS medium_pct,
    low_confidence,
    round(100.0 * low_confidence / total, 1) AS low_pct,
    no_embedding,
    round(100.0 * no_embedding / total, 1) AS no_embedding_pct,
    below_high_threshold,
    round(100.0 * below_high_threshold / total, 1) AS below_threshold_pct;

// Audit HAS_SUPPLIER edges
// High threshold: 0.55, Medium threshold: 0.30
MATCH (source:Company)-[r:HAS_SUPPLIER]->(target:Company)
WITH
    count(*) AS total,
    sum(CASE WHEN r.embedding_similarity IS NULL THEN 1 ELSE 0 END) AS no_embedding,
    sum(CASE WHEN r.embedding_similarity >= 0.55 THEN 1 ELSE 0 END) AS high_confidence,
    sum(CASE WHEN r.embedding_similarity >= 0.30 AND r.embedding_similarity < 0.55 THEN 1 ELSE 0 END) AS medium_confidence,
    sum(CASE WHEN r.embedding_similarity < 0.30 OR r.embedding_similarity IS NULL THEN 1 ELSE 0 END) AS low_confidence,
    sum(CASE WHEN r.embedding_similarity < 0.55 THEN 1 ELSE 0 END) AS below_high_threshold
RETURN
    'HAS_SUPPLIER' AS relationship_type,
    total,
    high_confidence,
    round(100.0 * high_confidence / total, 1) AS high_pct,
    medium_confidence,
    round(100.0 * medium_confidence / total, 1) AS medium_pct,
    low_confidence,
    round(100.0 * low_confidence / total, 1) AS low_pct,
    no_embedding,
    round(100.0 * no_embedding / total, 1) AS no_embedding_pct,
    below_high_threshold,
    round(100.0 * below_high_threshold / total, 1) AS below_threshold_pct;

// Audit HAS_CUSTOMER edges
// High threshold: 0.55, Medium threshold: 0.30
MATCH (source:Company)-[r:HAS_CUSTOMER]->(target:Company)
WITH
    count(*) AS total,
    sum(CASE WHEN r.embedding_similarity IS NULL THEN 1 ELSE 0 END) AS no_embedding,
    sum(CASE WHEN r.embedding_similarity >= 0.55 THEN 1 ELSE 0 END) AS high_confidence,
    sum(CASE WHEN r.embedding_similarity >= 0.30 AND r.embedding_similarity < 0.55 THEN 1 ELSE 0 END) AS medium_confidence,
    sum(CASE WHEN r.embedding_similarity < 0.30 OR r.embedding_similarity IS NULL THEN 1 ELSE 0 END) AS low_confidence,
    sum(CASE WHEN r.embedding_similarity < 0.55 THEN 1 ELSE 0 END) AS below_high_threshold
RETURN
    'HAS_CUSTOMER' AS relationship_type,
    total,
    high_confidence,
    round(100.0 * high_confidence / total, 1) AS high_pct,
    medium_confidence,
    round(100.0 * medium_confidence / total, 1) AS medium_pct,
    low_confidence,
    round(100.0 * low_confidence / total, 1) AS low_pct,
    no_embedding,
    round(100.0 * no_embedding / total, 1) AS no_embedding_pct,
    below_high_threshold,
    round(100.0 * below_high_threshold / total, 1) AS below_threshold_pct;

// Summary: Find edges that should be converted to candidates or deleted
// (Below high threshold but above medium = convert to candidate)
// (Below medium threshold = delete)

MATCH (source:Company)-[r]->(target:Company)
WHERE type(r) IN ['HAS_COMPETITOR', 'HAS_PARTNER', 'HAS_SUPPLIER', 'HAS_CUSTOMER']
WITH type(r) AS rel_type, r,
    CASE type(r)
        WHEN 'HAS_COMPETITOR' THEN 0.35
        WHEN 'HAS_PARTNER' THEN 0.50
        WHEN 'HAS_SUPPLIER' THEN 0.55
        WHEN 'HAS_CUSTOMER' THEN 0.55
    END AS high_threshold,
    CASE type(r)
        WHEN 'HAS_COMPETITOR' THEN 0.25
        WHEN 'HAS_PARTNER' THEN 0.30
        WHEN 'HAS_SUPPLIER' THEN 0.30
        WHEN 'HAS_CUSTOMER' THEN 0.30
    END AS medium_threshold
WITH rel_type,
    sum(CASE
        WHEN r.embedding_similarity IS NOT NULL
         AND r.embedding_similarity < high_threshold
         AND r.embedding_similarity >= medium_threshold
        THEN 1 ELSE 0
    END) AS should_convert_to_candidate,
    sum(CASE
        WHEN (r.embedding_similarity IS NOT NULL AND r.embedding_similarity < medium_threshold)
         OR (r.embedding_similarity IS NULL AND (r.confidence IS NULL OR r.confidence < 0.5))
        THEN 1 ELSE 0
    END) AS should_delete
RETURN
    rel_type,
    should_convert_to_candidate,
    should_delete,
    should_convert_to_candidate + should_delete AS total_low_quality,
    round(100.0 * (should_convert_to_candidate + should_delete) /
          (should_convert_to_candidate + should_delete +
           (CASE WHEN should_convert_to_candidate + should_delete > 0 THEN 0 ELSE 1 END)), 1) AS low_quality_pct
ORDER BY rel_type;
