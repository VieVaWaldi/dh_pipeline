{{ config(
    materialized='table',
    post_hook='ALTER TABLE {{ this }} ADD PRIMARY KEY (id);'
) }}

WITH cordis_data AS (
    SELECT
        institution_id::TEXT as source_id,
        'cordis'::TEXT as source_system,
        legal_name,
        sme,
        url,
        short_name,
        vat_number,
        street,
        postbox,
        postalcode,
        city,
        country,
        geolocation,
        type_title,
        nuts_level_0,
        nuts_level_1,
        nuts_level_2,
        nuts_level_3,
        -- OpenAIRE specific columns (NULL for CORDIS)
        NULL::TEXT as original_id,
        NULL::BOOLEAN as is_first_listed,
        NULL::TEXT[] as alternative_names,
        NULL::TEXT as country_code,
        NULL::TEXT as country_label
    FROM {{ ref('int_cordis_institution_dedup') }}
),

openaire_data AS (
    SELECT
        organization_id::TEXT as source_id,
        'openaire'::TEXT as source_system,
        name as legal_name,
        NULL::BOOLEAN as sme,
        website_url as url,
        short_name,
        NULL::TEXT as vat_number,
        NULL::TEXT as street,
        NULL::TEXT as postbox,
        NULL::TEXT as postalcode,
        NULL::TEXT as city,
        NULL::TEXT as country,
        geolocation,
        NULL::TEXT as type_title,
        NULL::TEXT as nuts_level_0,
        NULL::TEXT as nuts_level_1,
        NULL::TEXT as nuts_level_2,
        NULL::TEXT as nuts_level_3,
        -- OpenAIRE specific columns
        original_id,
        is_first_listed,
        alternative_names,
        country_code,
        country_label
    FROM {{ ref('int_openaire_organization_dedup') }}
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
    legal_name,
    sme,
    url,
    short_name,
    vat_number,
    street,
    postbox,
    postalcode,
    city,
    country,
    geolocation,
    type_title,
    nuts_level_0,
    nuts_level_1,
    nuts_level_2,
    nuts_level_3,
    original_id,
    is_first_listed,
    alternative_names,
    country_code,
    country_label,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM combined
WHERE source_system IS NOT NULL
    AND source_id IS NOT NULL
ORDER BY source_system, source_id
