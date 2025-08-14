{{ config(
    materialized='view'
) }}

select
    id as author_id,
    full_name as name,
    first_name,
    surname,
    pid,
    created_at,
    updated_at
from {{ source('openaire', 'author') }}