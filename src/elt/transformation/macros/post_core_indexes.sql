-- Run this macro after dbt run with: dbt run-operation post_core_indexes

-- ERRORs:
-- Had to remove this because we have a duplicate prohect institution junction
-- -> "CREATE UNIQUE INDEX IF NOT EXISTS idx_j_project_institution_unique ON " ~ schema_name ~ ".j_project_institution (project_id, institution_id)",

{% macro post_core_indexes() %}
  {% set schema_name = target.schema %}
  {% set queries = [] %}

  -- =============================================================================
  -- PRIMARY KEY INDEXES (PostgreSQL automatically creates these, but let's be explicit)
  -- =============================================================================

  {% set pk_indexes = [
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_author_pk ON " ~ schema_name ~ ".author (id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_fundingprogramme_pk ON " ~ schema_name ~ ".fundingprogramme (id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_institution_pk ON " ~ schema_name ~ ".institution (id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_link_pk ON " ~ schema_name ~ ".link (id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_project_pk ON " ~ schema_name ~ ".project (id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_researchoutput_pk ON " ~ schema_name ~ ".researchoutput (id)"
  ] %}

  -- =============================================================================
  -- BASIC INDEXES FOR CORE ENTITIES
  -- =============================================================================
  -- Author indexes
  -- Fundingprogramme indexes
  -- Institution indexes
  -- Link indexes
  -- Project indexes
  -- Research Output indexes

  {% set basic_indexes = [
    "CREATE INDEX IF NOT EXISTS idx_author_source ON " ~ schema_name ~ ".author (source_system, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_author_name ON " ~ schema_name ~ ".author (name)",

    "CREATE INDEX IF NOT EXISTS idx_fundingprogramme_source ON " ~ schema_name ~ ".fundingprogramme (source_system, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_fundingprogramme_code ON " ~ schema_name ~ ".fundingprogramme (code)",
    "CREATE INDEX IF NOT EXISTS idx_fundingprogramme_title ON " ~ schema_name ~ ".fundingprogramme (title)",

    "CREATE INDEX IF NOT EXISTS idx_institution_source ON " ~ schema_name ~ ".institution (source_system, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_institution_country_code ON " ~ schema_name ~ ".institution (country_code)",
    "CREATE INDEX IF NOT EXISTS idx_institution_city ON " ~ schema_name ~ ".institution (city)",
    "CREATE INDEX IF NOT EXISTS idx_institution_country ON " ~ schema_name ~ ".institution (country)",
    "CREATE INDEX IF NOT EXISTS idx_institution_legal_name ON " ~ schema_name ~ ".institution (legal_name)",

    "CREATE INDEX IF NOT EXISTS idx_link_source ON " ~ schema_name ~ ".link (source_system, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_link_url ON " ~ schema_name ~ ".link (url)",
    "CREATE INDEX IF NOT EXISTS idx_link_type ON " ~ schema_name ~ ".link (type)",

    "CREATE INDEX IF NOT EXISTS idx_project_source ON " ~ schema_name ~ ".project (source_system, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_project_doi ON " ~ schema_name ~ ".project (doi)",
    "CREATE INDEX IF NOT EXISTS idx_project_start_date ON " ~ schema_name ~ ".project (start_date)",
    "CREATE INDEX IF NOT EXISTS idx_project_total_cost ON " ~ schema_name ~ ".project (total_cost)",
    "CREATE INDEX IF NOT EXISTS idx_project_funded_amount ON " ~ schema_name ~ ".project (funded_amount)",

    "CREATE INDEX IF NOT EXISTS idx_researchoutput_source ON " ~ schema_name ~ ".researchoutput (source_system, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_doi ON " ~ schema_name ~ ".researchoutput (doi)",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_publication_date ON " ~ schema_name ~ ".researchoutput (publication_date)",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_language_code ON " ~ schema_name ~ ".researchoutput (language_code)",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_journal_name ON " ~ schema_name ~ ".researchoutput (journal_name)",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_publisher ON " ~ schema_name ~ ".researchoutput (publisher)",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_citation_count ON " ~ schema_name ~ ".researchoutput (citation_count)",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_popularity ON " ~ schema_name ~ ".researchoutput (popularity)",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_type ON " ~ schema_name ~ ".researchoutput (type)"
  ] %}

  -- =============================================================================
  -- JUNCTION TABLE INDEXES (CRITICAL FOR PERFORMANCE)
  -- =============================================================================
  -- j_institution_author
  -- j_project_fundingprogramme
  -- j_project_institution
  -- j_project_link
  -- j_project_researchoutput
  -- j_researchoutput_author
  -- j_researchoutput_institution
  -- j_researchoutput_link

  {% set junction_indexes = [
    "CREATE INDEX IF NOT EXISTS idx_j_institution_author_institution ON " ~ schema_name ~ ".j_institution_author (institution_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_institution_author_author ON " ~ schema_name ~ ".j_institution_author (author_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_j_institution_author_unique ON " ~ schema_name ~ ".j_institution_author (institution_id, author_id)",

    "CREATE INDEX IF NOT EXISTS idx_j_project_fundingprogramme_project ON " ~ schema_name ~ ".j_project_fundingprogramme (project_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_project_fundingprogramme_funding ON " ~ schema_name ~ ".j_project_fundingprogramme (fundingprogramme_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_j_project_fundingprogramme_unique ON " ~ schema_name ~ ".j_project_fundingprogramme (project_id, fundingprogramme_id)",

    "CREATE INDEX IF NOT EXISTS idx_j_project_institution_project ON " ~ schema_name ~ ".j_project_institution (project_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_project_institution_institution ON " ~ schema_name ~ ".j_project_institution (institution_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_project_institution_position ON " ~ schema_name ~ ".j_project_institution (institution_position)",

    "CREATE INDEX IF NOT EXISTS idx_j_project_link_project ON " ~ schema_name ~ ".j_project_link (project_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_project_link_link ON " ~ schema_name ~ ".j_project_link (link_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_j_project_link_unique ON " ~ schema_name ~ ".j_project_link (project_id, link_id)",

    "CREATE INDEX IF NOT EXISTS idx_j_project_researchoutput_project ON " ~ schema_name ~ ".j_project_researchoutput (project_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_project_researchoutput_ro ON " ~ schema_name ~ ".j_project_researchoutput (researchoutput_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_j_project_researchoutput_unique ON " ~ schema_name ~ ".j_project_researchoutput (project_id, researchoutput_id)",

    "CREATE INDEX IF NOT EXISTS idx_j_researchoutput_author_ro ON " ~ schema_name ~ ".j_researchoutput_author (researchoutput_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_researchoutput_author_author ON " ~ schema_name ~ ".j_researchoutput_author (author_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_researchoutput_author_rank ON " ~ schema_name ~ ".j_researchoutput_author (rank)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_j_researchoutput_author_unique ON " ~ schema_name ~ ".j_researchoutput_author (researchoutput_id, author_id)",

    "CREATE INDEX IF NOT EXISTS idx_j_researchoutput_institution_ro ON " ~ schema_name ~ ".j_researchoutput_institution (researchoutput_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_researchoutput_institution_inst ON " ~ schema_name ~ ".j_researchoutput_institution (institution_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_j_researchoutput_institution_unique ON " ~ schema_name ~ ".j_researchoutput_institution (researchoutput_id, institution_id)",

    "CREATE INDEX IF NOT EXISTS idx_j_researchoutput_link_ro ON " ~ schema_name ~ ".j_researchoutput_link (researchoutput_id)",
    "CREATE INDEX IF NOT EXISTS idx_j_researchoutput_link_link ON " ~ schema_name ~ ".j_researchoutput_link (link_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_j_researchoutput_link_unique ON " ~ schema_name ~ ".j_researchoutput_link (researchoutput_id, link_id)"
  ] %}

  -- =============================================================================
  -- FULL-TEXT SEARCH INDEXES
  -- =============================================================================
  -- Individual column indexes for Project
  -- Combined weighted index for Project (title weighted higher than objective)
  -- Individual column indexes for Research Output
  -- Combined weighted index for Research Output (title > abstract > fulltext)

  {% set fulltext_indexes = [
    "CREATE INDEX IF NOT EXISTS idx_project_title_fts ON " ~ schema_name ~ ".project USING GIN (to_tsvector('english', COALESCE(title, '')))",
    "CREATE INDEX IF NOT EXISTS idx_project_objective_fts ON " ~ schema_name ~ ".project USING GIN (to_tsvector('english', COALESCE(objective, '')))",

    "CREATE INDEX IF NOT EXISTS idx_project_combined_fts ON " ~ schema_name ~ ".project USING GIN (
      (setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
       setweight(to_tsvector('english', COALESCE(objective, '')), 'B'))
    )",

    "CREATE INDEX IF NOT EXISTS idx_researchoutput_title_fts ON " ~ schema_name ~ ".researchoutput USING GIN (to_tsvector('english', COALESCE(title, '')))",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_abstract_fts ON " ~ schema_name ~ ".researchoutput USING GIN (to_tsvector('english', COALESCE(abstract, '')))",
    "CREATE INDEX IF NOT EXISTS idx_researchoutput_fulltext_fts ON " ~ schema_name ~ ".researchoutput USING GIN (to_tsvector('english', COALESCE(fulltext, '')))",

    "CREATE INDEX IF NOT EXISTS idx_researchoutput_title_abstract_fts ON " ~ schema_name ~ ".researchoutput USING GIN (
      (setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
       setweight(to_tsvector('english', COALESCE(abstract, '')), 'B'))
    )",

    "CREATE INDEX IF NOT EXISTS idx_researchoutput_combined_fts ON " ~ schema_name ~ ".researchoutput USING GIN (
      (setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
       setweight(to_tsvector('english', COALESCE(abstract, '')), 'B') ||
       setweight(to_tsvector('english', COALESCE(fulltext, '')), 'C'))
    )"
  ] %}

  -- =============================================================================
  -- EXECUTE ALL QUERIES
  -- =============================================================================

  {% set all_queries = pk_indexes + basic_indexes + junction_indexes + fulltext_indexes %}

  {% for query in all_queries %}
    {{ log("Executing: " ~ query, info=True) }}
    {% set result = run_query(query) %}
  {% endfor %}

  -- =============================================================================
  -- ANALYZE TABLES FOR UPDATED STATISTICS
  -- =============================================================================

  {% set analyze_queries = [
    "ANALYZE " ~ schema_name ~ ".author",
    "ANALYZE " ~ schema_name ~ ".fundingprogramme",
    "ANALYZE " ~ schema_name ~ ".institution",
    "ANALYZE " ~ schema_name ~ ".link",
    "ANALYZE " ~ schema_name ~ ".project",
    "ANALYZE " ~ schema_name ~ ".researchoutput",
    "ANALYZE " ~ schema_name ~ ".j_institution_author",
    "ANALYZE " ~ schema_name ~ ".j_project_fundingprogramme",
    "ANALYZE " ~ schema_name ~ ".j_project_institution",
    "ANALYZE " ~ schema_name ~ ".j_project_link",
    "ANALYZE " ~ schema_name ~ ".j_project_researchoutput",
    "ANALYZE " ~ schema_name ~ ".j_researchoutput_author",
    "ANALYZE " ~ schema_name ~ ".j_researchoutput_institution",
    "ANALYZE " ~ schema_name ~ ".j_researchoutput_link"
  ] %}

  {% for query in analyze_queries %}
    {{ log("Analyzing: " ~ query, info=True) }}
    {% set result = run_query(query) %}
  {% endfor %}

  {{ log("✅ All indexes and analysis complete! Total queries executed: " ~ (all_queries | length + analyze_queries | length), info=True) }}

{% endmacro %}