{{ config(
    materialized='view'
) }}

select
    id as container_id,
    name,
    issn_printed,
    issn_online,
    issn_linking,
    volume,
    issue,
    start_page,
    end_page,
    edition,
    conference_place,
    conference_date,
    created_at,
    updated_at
from {{ source('openaire', 'container') }}