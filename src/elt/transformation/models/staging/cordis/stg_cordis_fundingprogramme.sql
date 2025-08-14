{{ config(
    materialized='view',
) }}

select
    id as fundingprogramme_id,
    code,
    title,
    short_title,
    framework_programme,
    pga,
    rcn
from {{ source('cordis', 'fundingprogramme') }}