{{ config(
    materialized='view'
) }}

select
    project_id,
    funder_id,
    currency,
    funded_amount,
    total_cost
from {{ source('openaire', 'j_project_funder') }}
