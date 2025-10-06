{{ config(
    materialized='table',
    post_hook='ALTER TABLE {{ this }} ADD PRIMARY KEY (id);'
) }}

WITH cordis_data AS (
    SELECT
        fundingprogramme_id::TEXT as source_id,
        'cordis'::TEXT as source_system,
        code,
        title,
        short_title,
        framework_programme,
        pga,
        rcn
    FROM {{ ref('int_cordis_fundingprogramme_dedup') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['source_system', 'source_id']) }} as id,
    source_id,
    source_system,
    code,
    title,
    short_title,
    framework_programme,
    pga,
    rcn,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM cordis_data
WHERE source_system IS NOT NULL
    AND source_id IS NOT NULL
ORDER BY source_system, source_id