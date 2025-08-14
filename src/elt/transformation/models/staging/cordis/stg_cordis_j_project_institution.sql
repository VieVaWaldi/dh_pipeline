{{ config(
    materialized='view'
) }}

select
    project_id,
    institution_id,
    institution_position,
    ec_contribution,
    net_ec_contribution,
    total_cost,
    type,
    organization_id,
    rcn
from {{ source('cordis', 'j_project_institution') }}