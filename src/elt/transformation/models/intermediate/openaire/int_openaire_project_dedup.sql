{{ config(materialized='table') }}

WITH step1_filter_untitled AS (
  SELECT *
  FROM {{ ref('stg_openaire_project') }}
  WHERE title IS NOT NULL
    AND LOWER(TRIM(title)) != 'untitled'
),

step2_code_dedup AS (
  -- Apply code deduplication first (keep latest by updated_at)
  SELECT *
  FROM step1_filter_untitled
  WHERE project_id IN (
    SELECT project_id
    FROM (
      SELECT project_id,
             ROW_NUMBER() OVER (
               PARTITION BY code
               ORDER BY updated_at DESC, project_id DESC
             ) as rn
      FROM step1_filter_untitled
      WHERE code IS NOT NULL
    ) ranked
    WHERE rn = 1
  )
),

step3_title_dedup AS (
  -- Apply title deduplication to the code-deduped results
  SELECT *
  FROM step2_code_dedup
  WHERE project_id IN (
    SELECT project_id
    FROM (
      SELECT project_id,
             ROW_NUMBER() OVER (
               PARTITION BY LOWER(TRIM(title))
               ORDER BY updated_at DESC, project_id DESC
             ) as rn
      FROM step2_code_dedup
    ) ranked
    WHERE rn = 1
  )
),

-- Add funding information from j_project_funder
projects_with_funding AS (
  SELECT
    p.*,
    f.name as funder_name,
    f.short_name as funder_short_name,
    f.jurisdiction as funder_jurisdiction,
    jpf.currency,
    jpf.funded_amount as funder_funded_amount,
    jpf.total_cost as funder_total_cost
  FROM step3_title_dedup p
  LEFT JOIN {{ ref('stg_openaire_j_project_funder') }} jpf ON p.project_id = jpf.project_id
  LEFT JOIN {{ ref('stg_openaire_funder') }} f ON jpf.funder_id = f.funder_id
)

SELECT * FROM projects_with_funding