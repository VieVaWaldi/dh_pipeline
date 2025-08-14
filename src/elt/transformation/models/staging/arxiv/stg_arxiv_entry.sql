{{ config(
    materialized='view',
) }}

select
    id as entry_id,
    id_original,
    doi,
    title,
    summary as abstract,
    full_text as fulltext,
    journal_ref as journal,
    comment,
    primary_category,
    categories,
    published_date,
    updated_date
from {{ source('arxiv', 'entry') }}