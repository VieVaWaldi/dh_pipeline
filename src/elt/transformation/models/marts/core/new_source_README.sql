-- Use source_system + source_id for stable IDs
SELECT
ROW_NUMBER() OVER (ORDER BY source_system, source_id) as id,
-- This ensures arxiv records always get the same IDs


-- models/marts/core/researchoutput.sql
WITH arxiv_data AS (
    SELECT
        source_id,
        'arxiv' as source_system,
        title,
        abstract,
        -- ...
    FROM {{ ref('int_arxiv_entries_dedup') }}
),

cordis_data AS (
    SELECT
        source_id,
        'cordis' as source_system,
        title,
        abstract,
        -- ...
    FROM {{ ref('int_cordis_entries_dedup') }}
),

combined AS (
    SELECT * FROM arxiv_data
    UNION ALL
    SELECT * FROM cordis_data
)

SELECT
    ROW_NUMBER() OVER (ORDER BY source_system, source_id) as id,
    source_id,
    source_system,
    title,
    abstract,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM combined