{{ config(
    materialized='view'
) }}

select
    id as organization_id,
    original_id,
    legal_name as name,
    legal_short_name as short_name,
    is_first_listed,
    geolocation,
    alternative_names,
    website_url,
    country_code,
    country_label,
    created_at,
    updated_at
from {{ source('openaire', 'organization') }}