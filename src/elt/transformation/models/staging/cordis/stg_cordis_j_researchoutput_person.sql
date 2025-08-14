{{ config(
    materialized='view'
) }}

select
    researchoutput_id,
    person_id,
    person_position
from {{ source('cordis', 'j_researchoutput_person') }}
