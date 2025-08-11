{{ config(
    materialized='view'
) }}

select
    id as link_id,
    href as url,
    title,
    rel,
    type
from {{ source('arxiv', 'link') }}