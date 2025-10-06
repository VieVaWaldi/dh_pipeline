{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT researchoutput_link_pkey PRIMARY KEY (researchoutput_id, link_id)"
) }}


WITH arxiv_junctions AS (
  SELECT
      r.id as researchoutput_id,
      l.id as link_id
  FROM {{ ref('int_arxiv_j_entry_link_filtered') }} j
  JOIN {{ ref('researchoutput') }} r
      ON r.source_id = j.entry_id::TEXT AND r.source_system = 'arxiv'
  JOIN {{ ref('link') }} l
      ON l.source_id = j.link_id::TEXT AND l.source_system = 'arxiv'
),

cordis_junctions AS (
  SELECT DISTINCT
      r.id as researchoutput_id,
      l.id as link_id
  FROM {{ ref('int_cordis_j_researchoutput_weblink_filtered') }} j
  JOIN {{ ref('researchoutput') }} r
      ON r.source_id = j.researchoutput_id::TEXT AND r.source_system = 'cordis'
  JOIN {{ ref('link') }} l
      ON l.source_id = j.weblink_id::TEXT AND l.source_system = 'cordis'
)

SELECT * FROM arxiv_junctions
UNION ALL
SELECT * FROM cordis_junctions
ORDER BY researchoutput_id, link_id