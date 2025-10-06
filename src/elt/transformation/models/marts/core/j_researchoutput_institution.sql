{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT researchoutput_institution_pkey PRIMARY KEY (researchoutput_id, institution_id)"
) }}

WITH cordis_junctions AS (
  SELECT DISTINCT
      ro.id as researchoutput_id,
      i.id as institution_id,
      NULL::TEXT as relation_type,  -- OpenAIRE specific (NULL for CORDIS)
      NULL::TEXT as country_code,   -- OpenAIRE specific (NULL for CORDIS)
      NULL::TEXT as country_label   -- OpenAIRE specific (NULL for CORDIS)
  FROM {{ ref('int_cordis_j_researchoutput_institution_filtered') }} junction
  JOIN {{ ref('researchoutput') }} ro
    ON ro.source_id = junction.researchoutput_id::TEXT AND ro.source_system = 'cordis'
  JOIN {{ ref('institution') }} i
    ON i.source_id = junction.institution_id::TEXT AND i.source_system = 'cordis'
),

openaire_junctions AS (
  SELECT DISTINCT
      ro.id as researchoutput_id,
      i.id as institution_id,
      junction.relation_type,  -- OpenAIRE specific
      junction.country_code,   -- OpenAIRE specific
      junction.country_label   -- OpenAIRE specific
  FROM {{ ref('int_openaire_j_researchoutput_organization_filtered') }} junction
  JOIN {{ ref('researchoutput') }} ro
    ON ro.source_id = junction.researchoutput_id::TEXT AND ro.source_system = 'openaire'
  JOIN {{ ref('institution') }} i
    ON i.source_id = junction.organization_id::TEXT AND i.source_system = 'openaire'
),

combined AS (
  SELECT * FROM cordis_junctions
  UNION ALL
  SELECT * FROM openaire_junctions
)

SELECT
  researchoutput_id,
  institution_id,
  relation_type,
  country_code,
  country_label
FROM combined
ORDER BY researchoutput_id, institution_id