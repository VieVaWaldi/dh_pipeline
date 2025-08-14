{{ config(
    materialized='view',
) }}

select
    id as project_id,
    id_original,
    id_openaire,
    code,
    title,
    doi,
    acronym,
    start_date,
    end_date,
    duration,
    summary,
    keywords,
    ec_article29_3,
    open_access_mandate_publications,
    open_access_mandate_dataset,
    ecsc39,
    total_cost,
    funded_amount,
    website_url,
    call_identifier,
    created_at,
    updated_at
from {{ source('openaire', 'project') }}