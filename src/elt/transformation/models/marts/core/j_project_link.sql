{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT project_link_pkey PRIMARY KEY (project_id, link_id)"
) }}

WITH cordis_junctions AS (
  SELECT DISTINCT
      p.id as project_id,
      l.id as link_id
  FROM {{ ref('int_cordis_j_project_weblink_filtered') }} junction
  JOIN {{ ref('project') }} p
    ON p.source_id = junction.project_id::TEXT AND p.source_system = 'cordis'
  JOIN {{ ref('link') }} l
    ON l.source_id = junction.weblink_id::TEXT AND l.source_system = 'cordis'
)

SELECT
  project_id,
  link_id
FROM cordis_junctions
ORDER BY project_id, link_id