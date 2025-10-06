{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT institution_author_pkey PRIMARY KEY (institution_id, author_id)"
) }}

WITH cordis_junctions AS (
  SELECT DISTINCT
      i.id as institution_id,
      a.id as author_id
  FROM {{ ref('int_cordis_j_institution_person_filtered') }} junction
  JOIN {{ ref('institution') }} i
    ON i.source_id = junction.institution_id::TEXT AND i.source_system = 'cordis'
  JOIN {{ ref('author') }} a
    ON a.source_id = junction.person_id::TEXT AND a.source_system = 'cordis'
)

SELECT
  institution_id,
  author_id
FROM cordis_junctions
ORDER BY institution_id, author_id