{{ config(
    materialized='view'
) }}

select
    researchoutput_id,
    weblink_id
from {{ source('cordis', 'j_researchoutput_weblink') }}
