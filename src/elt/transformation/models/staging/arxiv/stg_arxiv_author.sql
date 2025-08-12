{{ config(
    materialized='view'
) }}

select
    id as author_id,
    name,
    -- affiliation, -- Duplicate of affiliations
    affiliations
from {{ source('arxiv', 'author') }}