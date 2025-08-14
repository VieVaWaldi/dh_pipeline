{{ config(
    materialized='view',
) }}

select
    id as project_id,
    id_original,
    doi,
    title,
    acronym,
    status,
    start_date,
    end_date,
    ec_signature_date,
    total_cost,
    ec_max_contribution,
    objective,
    call_identifier,
    call_title,
    call_rcn
from {{ source('cordis', 'project') }}