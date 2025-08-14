{{ config(materialized='table') }}

SELECT ea.*
FROM {{ ref('stg_arxiv_j_entry_author') }} ea
WHERE ea.entry_id IN (
    SELECT entry_id
    FROM {{ ref('int_arxiv_entry_dedup') }}
)