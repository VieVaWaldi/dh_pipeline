{{ config(
    materialized='view',
) }}

select
    id as weblink_id,
    url,
    title,
    'external'::TEXT as type
from {{ source('cordis', 'weblink') }}