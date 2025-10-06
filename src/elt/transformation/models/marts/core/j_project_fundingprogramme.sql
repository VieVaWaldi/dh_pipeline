{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT project_fundingprogramme_pkey PRIMARY KEY (project_id, fundingprogramme_id)"
) }}

WITH cordis_junctions AS (
  SELECT DISTINCT
      p.id as project_id,
      f.id as fundingprogramme_id
  FROM {{ ref('int_cordis_j_project_fundingprogramme_filtered') }} junction
  JOIN {{ ref('project') }} p
    ON p.source_id = junction.project_id::TEXT AND p.source_system = 'cordis'
  JOIN {{ ref('fundingprogramme') }} f
    ON f.source_id = junction.fundingprogramme_id::TEXT AND f.source_system = 'cordis'
)

SELECT
  project_id,
  fundingprogramme_id
FROM cordis_junctions
ORDER BY project_id, fundingprogramme_id