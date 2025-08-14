{{ config(
    materialized='view'
) }}

select
    project_id,
    h2020_programme_id
from {{ source('openaire', 'j_project_h2020_programme') }}