{{ config(materialized='table') }}

WITH step1_doi AS (
  {{ dedup_by_doi(ref('stg_cordis_project'), id_col='project_id', doi_col='doi', updated_col='start_date') }}
),

step2_title AS (
  -- Apply title dedup to the DOI-deduped results
  SELECT *
  FROM step1_doi
  WHERE title IS NOT NULL
    AND TRIM(title) != ''
    AND project_id IN (
      SELECT project_id
      FROM (
        SELECT project_id,
               ROW_NUMBER() OVER (
                 PARTITION BY LOWER(TRIM(title))
                 ORDER BY start_date DESC
               ) as rn
        FROM step1_doi
        WHERE title IS NOT NULL
          AND TRIM(title) != ''
      ) ranked
      WHERE rn = 1
    )
)

SELECT * FROM step2_title