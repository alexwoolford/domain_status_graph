<agents_guidance>
  <global_rule>
    Fail fast on all critical preconditions (SQLite schema, Neo4j connectivity, constraints/indexes, dependency versions). No silent fallbacks.
  </global_rule>

  <neo4j_usage>
    <rule>Always use context managers for Driver/Session.</rule>
    <rule>Ensure constraints/indexes exist before any writes; abort if missing.</rule>
    <rule>Use MERGE for nodes/relationships; runs must be idempotent and re-runnable.</rule>
    <rule>No implicit data mutations; migrations/scripts are explicit and versioned.</rule>
  </neo4j_usage>

  <data_validation>
    <required_tables>url_status</required_tables>
    <recommended_tables>url_technologies, url_nameservers, url_mx_records, url_geoip</recommended_tables>
    <rule>Verify table presence + row counts before load; normalize key strings (lowercase hostnames/domains).</rule>
  </data_validation>

  <graph_schema>
    <nodes>
      Company(key=cik, props=ticker, name, description, description_embedding, risk_factors, risk_factors_embedding, sector, industry, sic_code, revenue, market_cap, employees, filing_date, filing_year, accession_number, sec_filing_url, data_source, data_updated_at, loaded_at, headquarters_city, headquarters_state, headquarters_country, fiscal_year_end, embedding_model, embedding_dimension)
      Domain(key=final_domain, props=domain, initial_domain, http_status, status, response_time, is_mobile_friendly, title, description, dmarc_record, spf_record, creation_date, expiration_date, registrar, registrant_org, loaded_at, description_embedding, keyword_embedding, embedding_model, embedding_dimension, keyword_embedding_model, keyword_embedding_dimension)
      Technology(key=name, props=category, loaded_at)
      Document(key=doc_id, props=company_cik, company_ticker, company_name, section_type, filing_year, chunk_count, created_at)
      Chunk(key=chunk_id, props=text, chunk_index, metadata, embedding, embedding_model, embedding_dimension, created_at)
    </nodes>
    <relationships>
      (:Company)-[:HAS_DOMAIN]->(:Domain)
      (:Company)-[:HAS]->(:Document)
      (:Company)-[:HAS_COMPETITOR {confidence, confidence_tier, raw_mention, context, source, extracted_at, embedding_similarity}]->(:Company)
      (:Company)-[:HAS_CUSTOMER {confidence, confidence_tier, raw_mention, context, source, extracted_at, embedding_similarity, llm_confidence, llm_verified}]->(:Company)
      (:Company)-[:HAS_SUPPLIER {confidence, confidence_tier, raw_mention, context, source, extracted_at, embedding_similarity, llm_confidence, llm_verified}]->(:Company)
      (:Company)-[:HAS_PARTNER {confidence, confidence_tier, raw_mention, context, source, extracted_at, embedding_similarity}]->(:Company)
      (:Company)-[:CANDIDATE_COMPETITOR|CANDIDATE_CUSTOMER|CANDIDATE_SUPPLIER|CANDIDATE_PARTNER {confidence, confidence_tier, raw_mention, context, source, extracted_at, embedding_similarity, llm_confidence?, llm_verified?}]->(:Company)
      (:Company)-[:SIMILAR_DESCRIPTION|SIMILAR_RISK|SIMILAR_INDUSTRY|SIMILAR_SIZE|SIMILAR_TECHNOLOGY|SIMILAR_KEYWORD {score, metric, computed_at, method?, classification?, bucket?}]->(:Company)
      (:Domain)-[:USES {loaded_at}]->(:Technology)
      (:Domain)-[:LIKELY_TO_ADOPT {score, computed_at, algorithm}]->(:Technology)
      (:Technology)-[:CO_OCCURS_WITH {similarity, metric, computed_at}]->(:Technology)
      (:Chunk)-[:PART_OF_DOCUMENT]->(:Document)
      (:Chunk)-[:NEXT_CHUNK]->(:Chunk)
    </relationships>
    <properties>
      Keep raw page/TLS/DNS fields on Domain (status, http_status, response_time, spf_record, dmarc_record, is_mobile_friendly).
      Stamp writes with loaded_at (UTC) and source="domain_status".
      Company nodes include risk_factors and risk_factors_embedding for similarity analysis.
      Document and Chunk nodes are for GraphRAG semantic search over filing text.
    </properties>
  </graph_schema>

  <constraints_and_indexes>
    CONSTRAINT unique_company IF NOT EXISTS ON (c:Company) ASSERT c.cik IS UNIQUE
    CONSTRAINT unique_domain IF NOT EXISTS ON (d:Domain) ASSERT d.final_domain IS UNIQUE
    CONSTRAINT unique_tech IF NOT EXISTS ON (t:Technology) ASSERT t.name IS UNIQUE
    CONSTRAINT unique_document IF NOT EXISTS ON (d:Document) ASSERT d.doc_id IS UNIQUE
    CONSTRAINT unique_chunk IF NOT EXISTS ON (c:Chunk) ASSERT c.chunk_id IS UNIQUE
    INDEX company_ticker ON (c:Company) FOR (c.ticker)
    INDEX company_sector ON (c:Company) FOR (c.sector)
    INDEX company_industry ON (c:Company) FOR (c.industry)
    INDEX company_filing_year ON (c:Company) FOR (c.filing_year)
    INDEX company_accession_number ON (c:Company) FOR (c.accession_number)
    INDEX document_company_cik ON (d:Document) FOR (d.company_cik)
    INDEX document_section_type ON (d:Document) FOR (d.section_type)
    INDEX chunk_chunk_index ON (c:Chunk) FOR (c.chunk_index)
    VECTOR INDEX chunk_embedding_vector ON (c:Chunk) FOR (c.embedding) OPTIONS {vector.dimensions: 1536, vector.similarity_function: 'cosine'}
  </constraints_and_indexes>

  <ingest_pipeline>
    <stage0>Dry-run: connect, check constraints, count tables, print plan; exit unless --execute.</stage0>
    <stage1>Load core nodes with UNWIND batching (e.g., 1000); normalize keys; MERGE only.</stage1>
    <stage2>Load relationships (USES, HAS_DOMAIN, HAS_COMPETITOR, HAS_CUSTOMER, HAS_SUPPLIER, HAS_PARTNER, SIMILAR_*, HAS, PART_OF_DOCUMENT, NEXT_CHUNK).</stage2>
    <stage3>Post-load sanity checks (counts, degrees) and summary log.</stage3>
    <batching>Use optimized UNWIND batching for large batches (5K per transaction); reliable and performant (1-2M relationships/minute).</batching>
  </ingest_pipeline>

  <gds_usage>
    <allowed>WCC, Louvain, Node Similarity (Domain↔Technology projection), PageRank</allowed>
    <rules>Create named in-memory graphs (e.g., "ds_main"); check memory; drop graphs after use; log timings.</rules>
  </gds_usage>

  <development_setup>
    <env>
      Always use the dedicated conda env: public_company_graph (Python 3.13)
      conda activate public_company_graph
      pip install -e .[dev]
      pip install -e .
      cp .env.sample .env  # set NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
    </env>
    <quality_tools>
      ruff check public_company_graph/ scripts/
      ruff format public_company_graph/ scripts/
      mypy public_company_graph/ --ignore-missing-imports
      pytest tests/ -v --cov=public_company_graph
      pre-commit run --all-files  # must pass locally and in CI
    </quality_tools>
  </development_setup>

  <testing>
    <rule>Integration tests for constraints and ingest; unit tests for helpers.</rule>
    <rule>Idempotency test: running importer twice yields identical counts.</rule>
  </testing>

  <ci_zero_tolerance>
    <golden_rule>pre-commit run --all-files must show "Passed" for all checks.</golden_rule>
  </ci_zero_tolerance>

  <architecture_overview>
    <entry file="scripts/bootstrap_graph.py" desc="Entry point: dry-run plan, then execute ingest"/>
    <entry file="public_company_graph/neo4j/constraints.py" desc="Constraint creation for Domain, Technology, Company nodes"/>
    <entry file="public_company_graph/ingest/sqlite_readers.py" desc="SQLite data readers for domain and technology data"/>
    <entry file="public_company_graph/ingest/loaders.py" desc="Neo4j batch loaders for Domain and Technology nodes"/>
    <entry file="public_company_graph/embeddings/" desc="Embedding creation, caching, and OpenAI client"/>
    <entry file="public_company_graph/cli.py" desc="Common CLI utilities (logging, dry-run, connection handling)"/>
    <entry file="public_company_graph/config.py" desc="Configuration management (Neo4j, OpenAI, data paths)"/>
    <entry file="public_company_graph/neo4j/connection.py" desc="Neo4j driver and session management"/>
    <entry file="docs/architecture.md" desc="Complete architecture documentation (package structure, design principles)"/>
    <entry file="docs/money_queries.md" desc="High-value Cypher query examples"/>
  </architecture_overview>
</agents_guidance>
