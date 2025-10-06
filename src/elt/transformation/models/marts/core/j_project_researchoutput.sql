{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT project_researchoutput_pkey PRIMARY KEY (project_id, researchoutput_id)"
) }}

WITH cordis_junctions AS (
  SELECT DISTINCT
      p.id as project_id,
      ro.id as researchoutput_id,
      NULL::TEXT as relation_type  -- OpenAIRE specific (NULL for CORDIS)
  FROM {{ ref('int_cordis_j_project_researchoutput_filtered') }} junction
  JOIN {{ ref('project') }} p
    ON p.source_id = junction.project_id::TEXT AND p.source_system = 'cordis'
  JOIN {{ ref('researchoutput') }} ro
    ON ro.source_id = junction.researchoutput_id::TEXT AND ro.source_system = 'cordis'
),

openaire_junctions AS (
  SELECT DISTINCT
      p.id as project_id,
      ro.id as researchoutput_id,
      junction.relation_type  -- OpenAIRE specific
  FROM {{ ref('int_openaire_j_project_researchoutput_filtered') }} junction
  JOIN {{ ref('project') }} p
    ON p.source_id = junction.project_id::TEXT AND p.source_system = 'openaire'
  JOIN {{ ref('researchoutput') }} ro
    ON ro.source_id = junction.researchoutput_id::TEXT AND ro.source_system = 'openaire'
),

combined AS (
  SELECT * FROM cordis_junctions
  UNION ALL
  SELECT * FROM openaire_junctions
)

SELECT
  project_id,
  researchoutput_id,
  relation_type
FROM combined
ORDER BY project_id, researchoutput_id