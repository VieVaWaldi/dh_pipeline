{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY url, link_id) as id,
    url,
    type,
    title as description,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref('int_arxiv_link_dedup') }}
WHERE url IS NOT NULL
ORDER BY url, link_id