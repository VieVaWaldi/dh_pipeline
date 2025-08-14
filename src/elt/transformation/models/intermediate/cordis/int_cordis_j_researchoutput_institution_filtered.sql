{{ config(materialized='table') }}

SELECT
  ri.researchoutput_id,
  mapping.mapped_id as institution_id
FROM {{ ref('stg_cordis_j_researchoutput_institution') }} ri
LEFT JOIN {{ ref('int_cordis_institution_dedup_mapping') }} mapping
  ON ri.institution_id = mapping.original_id
WHERE ri.researchoutput_id IN (
    SELECT researchoutput_id
    FROM {{ ref('int_cordis_researchoutput_dedup') }}
)
  AND mapping.mapped_id IN (
    SELECT institution_id FROM {{ ref('int_cordis_institution_dedup') }}
  )