{{ config(
    materialized='view',
) }}

select
    id as institution_id,
    legal_name,
    --legal_name as name, -- Also map to name for core consistency -> NO
    sme,
    url,
    short_name,
    vat_number,
    street,
    postbox,
    postalcode,
    city,
    country,
    geolocation,
    type_title,
    nuts_level_0,
    nuts_level_1,
    nuts_level_2,
    nuts_level_3
from {{ source('cordis', 'institution') }}