{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT researchoutput_author_pkey PRIMARY KEY (researchoutput_id, author_id)"
) }}

WITH arxiv_junctions AS (
  SELECT DISTINCT
      ro.id as researchoutput_id,
      a.id as author_id,
      junction.author_position::DECIMAL as rank  -- Convert to DECIMAL for consistency
  FROM {{ ref('int_arxiv_j_entry_author_filtered') }} junction
  JOIN {{ ref('researchoutput') }} ro ON ro.source_id = junction.entry_id::TEXT AND ro.source_system = 'arxiv'
  JOIN {{ ref('author') }} a ON a.source_id = junction.author_id::TEXT AND a.source_system = 'arxiv'
),

cordis_junctions AS (
  SELECT DISTINCT
      ro.id as researchoutput_id,
      a.id as author_id,
      junction.person_position::DECIMAL as rank  -- Convert to DECIMAL for consistency
  FROM {{ ref('int_cordis_j_researchoutput_person_filtered') }} junction
  JOIN {{ ref('researchoutput') }} ro ON ro.source_id = junction.researchoutput_id::TEXT AND ro.source_system = 'cordis'
  JOIN {{ ref('author') }} a ON a.source_id = junction.person_id::TEXT AND a.source_system = 'cordis'
),

openaire_junctions AS (
  SELECT DISTINCT
      ro.id as researchoutput_id,
      a.id as author_id,
      junction.author_position as rank  -- Already DECIMAL in OpenAIRE
  FROM {{ ref('int_openaire_j_researchoutput_author_filtered') }} junction
  JOIN {{ ref('researchoutput') }} ro ON ro.source_id = junction.researchoutput_id::TEXT AND ro.source_system = 'openaire'
  JOIN {{ ref('author') }} a ON a.source_id = junction.author_id::TEXT AND a.source_system = 'openaire'
),

combined AS (
  SELECT * FROM arxiv_junctions
  UNION ALL
  SELECT * FROM cordis_junctions
  UNION ALL
  SELECT * FROM openaire_junctions
)

SELECT
  researchoutput_id,
  author_id,
  rank
FROM combined
ORDER BY researchoutput_id, rank, author_id