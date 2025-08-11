{{ config(materialized='table') }}

-- ToDo:
-- Author has affiliations which we need to turn into institutions.
-- author.affiiliations is an array that can be null

SELECT
    ROW_NUMBER() OVER (ORDER BY name) as id,
    name,
    'arxiv' as source_system,
    author_id::TEXT as source_id,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref('int_arxiv_authors_dedup') }}
WHERE name IS NOT NULL
    AND TRIM(name) != ''
ORDER BY name