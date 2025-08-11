{{ config(materialized='table') }}

SELECT el.*
FROM {{ ref('stg_arxiv_entry_links') }} el
WHERE el.entry_id IN (
    SELECT entry_id
    FROM {{ ref('int_arxiv_entries_dedup') }}
)