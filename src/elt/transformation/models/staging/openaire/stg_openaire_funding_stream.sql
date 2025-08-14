{{ config(
    materialized='view'
) }}

select
    id as funding_stream_id,
    original_id,
    name,
    description,
    parent_id,
    created_at,
    updated_at
from {{ source('openaire', 'funding_stream') }}
