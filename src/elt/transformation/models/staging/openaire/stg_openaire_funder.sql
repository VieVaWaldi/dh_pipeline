{{ config(
    materialized='view'
) }}

select
    id as funder_id,
    original_id,
    name,
    short_name,
    jurisdiction,
    created_at,
    updated_at
from {{ source('openaire', 'funder') }}