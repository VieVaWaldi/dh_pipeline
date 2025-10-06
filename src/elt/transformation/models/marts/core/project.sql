{{ config(
    materialized='table',
    post_hook='ALTER TABLE {{ this }} ADD PRIMARY KEY (id);'
) }}

WITH cordis_data AS (
    SELECT
        project_id::TEXT as source_id,
        'cordis'::TEXT as source_system,
        id_original as cordis_id,
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
        call_rcn,
        -- OpenAIRE specific columns (NULL for CORDIS)
        NULL::TEXT as id_original,
        NULL::TEXT as id_openaire,
        NULL::TEXT as code,
        NULL::TEXT as duration,
        NULL::TEXT as keywords,
        NULL::DECIMAL as funded_amount,
        NULL::TEXT as website_url,
        NULL::TEXT as funder_name,
        NULL::TEXT as funder_short_name,
        NULL::TEXT as funder_jurisdiction,
        NULL::TEXT as currency, -- will be Euro i guess
        NULL::DECIMAL as funder_total_cost
    FROM {{ ref('int_cordis_project_dedup') }}
),

openaire_data AS (
    SELECT
        project_id::TEXT as source_id,
        'openaire'::TEXT as source_system,
        NULL::TEXT as cordis_id,
        doi,
        title,
        acronym,
        NULL::TEXT as status,
        start_date,
        end_date,
        NULL::DATE as ec_signature_date,
        total_cost,
        NULL::DECIMAL as ec_max_contribution,
        summary as objective,
        call_identifier,
        NULL::TEXT as call_title,
        NULL::TEXT as call_rcn,
        -- OpenAIRE specific columns
        id_original,
        id_openaire,
        code,
        duration,
        keywords,
        funded_amount,
        website_url,
        funder_name,
        funder_short_name,
        funder_jurisdiction,
        currency,
        funder_total_cost
    FROM {{ ref('int_openaire_project_dedup') }}
),

combined AS (
    SELECT * FROM cordis_data
    UNION ALL
    SELECT * FROM openaire_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['source_system', 'source_id']) }} as id,
    source_id,
    source_system,
    cordis_id,
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
    call_rcn,
    id_original,
    id_openaire,
    code,
    duration,
    keywords,
    funded_amount,
    website_url,
    funder_name,
    funder_short_name,
    funder_jurisdiction,
    currency,
    funder_total_cost,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM combined
WHERE source_system IS NOT NULL
    AND source_id IS NOT NULL
ORDER BY source_system, source_id