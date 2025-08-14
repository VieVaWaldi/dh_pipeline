{{ config(materialized='table') }}

SELECT
  pi.project_id,
  mapping.mapped_id as institution_id,
  pi.institution_position,
  pi.ec_contribution,
  pi.net_ec_contribution,
  pi.total_cost,
  pi.type,
  pi.organization_id,
  pi.rcn
FROM {{ ref('stg_cordis_j_project_institution') }} pi
LEFT JOIN {{ ref('int_cordis_institution_dedup_mapping') }} mapping
  ON pi.institution_id = mapping.original_id
WHERE pi.project_id IN (
    SELECT project_id
    FROM {{ ref('int_cordis_project_dedup') }}
)
  AND mapping.mapped_id IN (
    SELECT institution_id FROM {{ ref('int_cordis_institution_dedup') }}
  )