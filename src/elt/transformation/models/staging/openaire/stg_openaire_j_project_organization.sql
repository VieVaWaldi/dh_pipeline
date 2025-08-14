{{ config(
    materialized='view'
) }}

select
    project_id,
    organization_id,
    relation_type,
    validation_date,
    validated
from {{ source('openaire', 'j_project_organization') }}