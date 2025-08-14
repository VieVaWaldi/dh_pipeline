{{ config(
    materialized='view'
) }}

select
    entry_id,
    link_id
from {{ source('arxiv', 'j_entry_link') }}