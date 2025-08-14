{{ config(
    materialized='view'
) }}

select
    project_id,
    funding_stream_id
from {{ source('openaire', 'j_project_funding_stream') }}