{{ config(
    materialized='table',
    post_hook='ALTER TABLE {{ this }} ADD PRIMARY KEY (id);'
) }}

WITH arxiv_data AS (
    SELECT
        link_id::TEXT as source_id,
        'arxiv'::TEXT as source_system,
        url,
        NULL::TEXT as title,
        type
    FROM {{ ref('int_arxiv_link_dedup') }}
),

cordis_data AS (
    SELECT
        weblink_id::TEXT as source_id,
        'cordis'::TEXT as source_system,
        url,
        title,
        type
    FROM {{ ref('int_cordis_weblink_dedup') }}
),

combined AS (
    SELECT * FROM arxiv_data
    UNION ALL
    SELECT * FROM cordis_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['source_system', 'source_id']) }} as id,
    source_id,
    source_system,
    url,
    title,
    type,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM combined
WHERE source_system IS NOT NULL
    AND source_id IS NOT NULL
ORDER BY source_system, source_id