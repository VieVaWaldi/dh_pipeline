{{ config(
    materialized='view',
) }}

select
    id as person_id,
    title,
    name,
    first_name,
    last_name,
    telephone_number
from {{ source('cordis', 'person') }}