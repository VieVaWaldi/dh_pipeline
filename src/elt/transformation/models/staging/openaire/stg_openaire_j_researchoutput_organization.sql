{{ config(
    materialized='view'
) }}

select
    research_output_id as researchoutput_id,
    organization_id,
    relation_type,
    country_code,
    country_label
from {{ source('openaire', 'j_researchoutput_organization') }}