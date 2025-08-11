{{ config(materialized='table') }}

SELECT 
    r.id as researchoutput_id,
    l.id as link_id
FROM {{ ref('int_arxiv_entry_links_filtered') }} j

JOIN {{ ref('researchoutput') }} r
    ON r.source_id = j.entry_id::TEXT
JOIN {{ ref('int_arxiv_links_dedup') }} orig_link
    ON orig_link.link_id = j.link_id
JOIN {{ ref('link') }} l 
    ON l.url = orig_link.url

ORDER BY r.id, l.id