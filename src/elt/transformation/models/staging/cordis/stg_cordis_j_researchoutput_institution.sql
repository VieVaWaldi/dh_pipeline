{{ config(
    materialized='view'
) }}

select
    researchoutput_id,
    institution_id
from {{ source('cordis', 'j_researchoutput_institution') }}