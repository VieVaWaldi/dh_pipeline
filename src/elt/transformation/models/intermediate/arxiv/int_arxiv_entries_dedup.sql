{{ config(materialized='table') }}

WITH step1_doi AS (
  {{ dedup_by_doi(ref('stg_arxiv_entries', id_col='entry_id', doi_col='doi', updated_col='updated_date')) }}
),

step2_title AS (
  -- Apply title dedup to the DOI-deduped results
  SELECT *
  FROM step1_doi
  WHERE title IS NOT NULL
    AND TRIM(title) != ''
    AND entry_id IN (
      SELECT entry_id
      FROM (
        SELECT entry_id,
               ROW_NUMBER() OVER (
                 PARTITION BY LOWER(TRIM(title))
                 ORDER BY updated_date DESC
               ) as rn
        FROM step1_doi
        WHERE title IS NOT NULL
          AND TRIM(title) != ''
      ) ranked
      WHERE rn = 1
    )
),

step3_arxiv_version AS (
  -- Apply ArXiv version dedup to the title-deduped results
  SELECT *
  FROM step2_title
  WHERE entry_id IN (
    SELECT entry_id
    FROM (
      SELECT entry_id,
             ROW_NUMBER() OVER (
               PARTITION BY REGEXP_REPLACE(id_original, 'v\d+$', '')
               ORDER BY updated_date DESC, entry_id DESC
             ) as rn
      FROM step2_title
    ) ranked
    WHERE rn = 1
  )
)

SELECT * FROM step3_arxiv_version