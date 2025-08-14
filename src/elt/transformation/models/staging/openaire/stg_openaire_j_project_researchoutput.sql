{{ config(
    materialized='view'
) }}

select
    project_id,
    researchoutput_id,
    relation_type
from {{ source('openaire', 'j_project_researchoutput') }}