{{ config(
    materialized='view'
) }}

select
    id as h2020_programme_id,
    code,
    description,
    created_at,
    updated_at
from {{ source('openaire', 'h2020_programme') }}
