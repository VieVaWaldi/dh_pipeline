{{ config(
    materialized='view'
) }}

select
    institution_id,
    person_id
from {{ source('cordis', 'j_institution_person') }}
