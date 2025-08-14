{{ config(
    materialized='view'
) }}

select
    project_id,
    weblink_id
from {{ source('cordis', 'j_project_weblink') }}
