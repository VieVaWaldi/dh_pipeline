{{ config(materialized='table') }}

SELECT ea.*
FROM {{ ref('stg_arxiv_entry_authors') }} ea
WHERE ea.entry_id IN (
    SELECT entry_id
    FROM {{ ref('int_arxiv_entries_dedup') }}
)