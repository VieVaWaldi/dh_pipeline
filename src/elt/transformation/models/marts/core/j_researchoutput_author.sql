{{ config(materialized='table') }}

SELECT DISTINCT
    ro.id as researchoutput_id,
    a.id as author_id
FROM {{ ref('int_arxiv_entry_authors_filtered') }} junction
JOIN {{ ref('researchoutput') }} ro ON ro.source_id = junction.entry_id::TEXT
JOIN {{ ref('author') }} a ON a.source_id = junction.author_id::TEXT