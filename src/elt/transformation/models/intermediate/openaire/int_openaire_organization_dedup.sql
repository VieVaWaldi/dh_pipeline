{{ config(materialized='table') }}

WITH legal_name_dedup AS (
  -- Simple deduplication by case-insensitive legal name
  SELECT *
  FROM {{ ref('stg_openaire_organization') }}
  WHERE organization_id IN (
    SELECT organization_id
    FROM (
      SELECT organization_id,
             ROW_NUMBER() OVER (
               PARTITION BY LOWER(TRIM(name))
               ORDER BY updated_at DESC, organization_id DESC
             ) as rn
      FROM {{ ref('stg_openaire_organization') }}
      WHERE name IS NOT NULL
        AND TRIM(name) != ''
    ) ranked
    WHERE rn = 1
  )
)

SELECT * FROM legal_name_dedup