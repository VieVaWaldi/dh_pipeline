{{ config(
    materialized='view'
) }}

select
    project_id,
    researchoutput_id
from {{ source('cordis', 'j_project_researchoutput') }}