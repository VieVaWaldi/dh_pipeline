-- models/intermediate/cordis/int_cordis_institution_dedup_mapping.sql

{{ config(materialized='table') }}

WITH canonical_institutions AS (
  SELECT
    institution_id as canonical_id,
    legal_name,
    url
  FROM {{ ref('int_cordis_institution_dedup') }}
),

all_institutions AS (
  SELECT
    institution_id,
    legal_name,
    url
  FROM {{ ref('stg_cordis_institution') }}
),

mapped_by_legal_name AS (
  SELECT
    ai.institution_id as original_id,
    ci.canonical_id as mapped_id,
    'legal_name' as mapping_reason
  FROM all_institutions ai
  JOIN canonical_institutions ci
    ON LOWER(TRIM(ai.legal_name)) = LOWER(TRIM(ci.legal_name))
  WHERE ai.legal_name IS NOT NULL AND TRIM(ai.legal_name) != ''
),

mapped_by_url AS (
  SELECT
    ai.institution_id as original_id,
    ci.canonical_id as mapped_id,
    'url' as mapping_reason
  FROM all_institutions ai
  JOIN canonical_institutions ci
    ON LOWER(TRIM(ai.url)) = LOWER(TRIM(ci.url))
  WHERE ai.url IS NOT NULL
    AND TRIM(ai.url) != ''
    AND ai.institution_id NOT IN (SELECT original_id FROM mapped_by_legal_name)
),

combined_mapping AS (
  SELECT original_id, mapped_id, mapping_reason FROM mapped_by_legal_name
  UNION ALL
  SELECT original_id, mapped_id, mapping_reason FROM mapped_by_url
)

SELECT
  ai.institution_id as original_id,
  COALESCE(cm.mapped_id, ai.institution_id) as mapped_id,
  COALESCE(cm.mapping_reason, 'no_mapping') as mapping_reason
FROM all_institutions ai
LEFT JOIN combined_mapping cm ON ai.institution_id = cm.original_id