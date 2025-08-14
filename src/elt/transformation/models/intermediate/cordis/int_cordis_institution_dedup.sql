{{ config(materialized='table') }}

WITH step1_legal_name AS (
  -- Deduplicate by case-insensitive legal_name
  SELECT *
  FROM {{ ref('stg_cordis_institution') }}
  WHERE legal_name IS NOT NULL
    AND TRIM(legal_name) != ''
    AND institution_id IN (
      SELECT institution_id
      FROM (
        SELECT institution_id,
               ROW_NUMBER() OVER (
                 PARTITION BY LOWER(TRIM(legal_name))
                 ORDER BY institution_id ASC
               ) as rn
        FROM {{ ref('stg_cordis_institution') }}
        WHERE legal_name IS NOT NULL
          AND TRIM(legal_name) != ''
      ) ranked
      WHERE rn = 1
    )
),

step2_url AS (
  -- Apply URL deduplication to the legal_name-deduped results
  SELECT *
  FROM step1_legal_name
  WHERE institution_id IN (
    SELECT institution_id
    FROM (
      SELECT institution_id,
             ROW_NUMBER() OVER (
               PARTITION BY LOWER(TRIM(url))
               ORDER BY institution_id ASC
             ) as rn
      FROM step1_legal_name
      WHERE url IS NOT NULL
        AND TRIM(url) != ''
    ) ranked
    WHERE rn = 1
  )

  UNION ALL

  -- Keep institutions without URLs (can't be deduplicated by URL)
  SELECT *
  FROM step1_legal_name
  WHERE url IS NULL OR TRIM(url) = ''
)

SELECT * FROM step2_url