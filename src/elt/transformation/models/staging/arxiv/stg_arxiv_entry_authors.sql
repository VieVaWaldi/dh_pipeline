{{ config(
    materialized='view'
) }}

select
    entry_id,
    author_id,
    author_position
from {{ source('arxiv', 'j_entry_author') }}