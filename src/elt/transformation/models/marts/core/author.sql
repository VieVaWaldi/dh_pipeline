{{ config(
    materialized='table',
    post_hook='ALTER TABLE {{ this }} ADD PRIMARY KEY (id);'
) }}

-- ToDo:
-- Author has affiliations which we need to turn into institutions.
-- author.affiiliations is an array that can be null

WITH arxiv_authors AS (
    SELECT
        name,
        'arxiv' as source_system,
        author_id::TEXT as source_id
    FROM {{ ref('int_arxiv_author_dedup') }}
    WHERE name IS NOT NULL
        AND TRIM(name) != ''
),

cordis_authors AS (
    SELECT
        constructed_name as name,
        'cordis' as source_system,
        person_id::TEXT as source_id
    FROM (
        SELECT
            person_id,
            CASE
                WHEN name IS NOT NULL THEN name
                WHEN first_name IS NOT NULL AND last_name IS NOT NULL
                THEN TRIM(first_name || ' ' || last_name)
                ELSE NULL
            END as constructed_name
        FROM {{ ref('int_cordis_person_dedup') }}
    ) subq
    WHERE constructed_name IS NOT NULL
        AND TRIM(constructed_name) != ''
),

openaire_authors AS (
    SELECT
        name,
        'openaire' as source_system,
        author_id::TEXT as source_id
    FROM {{ ref('int_openaire_author_dedup') }}
    WHERE name IS NOT NULL
        AND TRIM(name) != ''
),

combined AS (
    SELECT * FROM arxiv_authors
    UNION ALL
    SELECT * FROM cordis_authors
    UNION ALL
    SELECT * FROM openaire_authors
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['source_system', 'source_id']) }} as id,
    name,
    source_system,
    source_id,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM combined
WHERE source_system IS NOT NULL
    AND source_id IS NOT NULL
ORDER BY source_system, source_id