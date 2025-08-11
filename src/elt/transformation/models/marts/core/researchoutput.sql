{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY entry_id) as id,

    entry_id::TEXT as source_id,
    'arxiv'::TEXT as source_system,

    doi,
    id_original as arxiv_id,

    published_date as publication_date,
    updated_date,

    'publication'::TEXT as type,
    NULL::TEXT as language_code,
    title,
    abstract,
    fulltext,
    comment,

    NULL::TEXT as funding_number,

    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM {{ ref('int_arxiv_entries_dedup') }}
ORDER BY entry_id