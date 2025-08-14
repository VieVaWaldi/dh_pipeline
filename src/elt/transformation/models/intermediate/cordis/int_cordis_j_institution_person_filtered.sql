{{ config(materialized='table') }}

SELECT
  mapping.mapped_id as institution_id,
  ip.person_id
FROM {{ ref('stg_cordis_j_institution_person') }} ip
LEFT JOIN {{ ref('int_cordis_institution_dedup_mapping') }} mapping
  ON ip.institution_id = mapping.original_id
WHERE mapping.mapped_id IN (
  SELECT institution_id FROM {{ ref('int_cordis_institution_dedup') }}
)