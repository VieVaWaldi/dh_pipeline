{{ config(materialized='table') }}

WITH step1_doi AS (
  -- Apply DOI deduplication - keep most recent for DOI duplicates
  {{ dedup_by_doi(ref('stg_cordis_researchoutput'), id_col='researchoutput_id', doi_col='doi', updated_col='publication_date') }}
)

SELECT * FROM step1_doi

-- TODO: Add title deduplication once full dataset is available
-- Need to create ignore list for generic titles like:
-- - "data management plan"
-- - "project website"
-- - "attachment_{number}"
-- step3_title AS (
--   SELECT *
--   FROM step1_doi
--   WHERE title IS NOT NULL
--     AND TRIM(title) != ''
--     AND LOWER(TRIM(title)) NOT IN (
--       -- Add ignore list here
--       'data management plan',
--       'project website'
--     )
--     AND researchoutput_id IN (
--       SELECT researchoutput_id
--       FROM (
--         SELECT researchoutput_id,
--                ROW_NUMBER() OVER (
--                  PARTITION BY LOWER(TRIM(title))
--                  ORDER BY updated_at DESC
--                ) as rn
--         FROM step1_doi
--         WHERE title IS NOT NULL
--           AND TRIM(title) != ''
--           AND LOWER(TRIM(title)) NOT IN (
--             -- Add ignore list here
--           )
--       ) ranked
--       WHERE rn = 1
--     )
-- )

-- TODO: Add version deduplication if version patterns are found
-- step4_version AS (
--   SELECT *
--   FROM step3_title
--   WHERE researchoutput_id IN (
--     SELECT researchoutput_id
--     FROM (
--       SELECT researchoutput_id,
--              ROW_NUMBER() OVER (
--                PARTITION BY REGEXP_REPLACE(id_original, 'v\d+$', '')
--                ORDER BY updated_at DESC, researchoutput_id DESC
--              ) as rn
--       FROM step3_title
--     ) ranked
--     WHERE rn = 1
--   )
-- )